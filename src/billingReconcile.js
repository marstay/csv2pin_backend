/**
 * Billing reconciliation — make Supabase agree with Dodo.
 *
 * Everything in this system is driven by webhooks arriving exactly once. When one is missed
 * (deploy, timeout, Dodo giving up on retries) the account drifts permanently; there is no
 * self-healing beyond the lazy current_period_end check. This is that self-healing.
 *
 * Dodo is the source of truth for WHETHER someone is paying and WHAT for — via product_id,
 * which cannot go stale the way subscription metadata can (see the 2026-07-30 incident where
 * a paying Agency customer was reverted to Creator limits on renewal).
 *
 * Shared by the in-process nightly job (src/index.js) and the CLI
 * (scripts/reconcile-billing.mjs) so both behave identically.
 */

// MUST MATCH PLAN_PIN_LIMITS in index.js. Reconciliation compares this against each row's
// pins_limit_per_month and REWRITES the row when they differ, so a stale copy raises a CRITICAL
// plan-drift alert for every customer and writes the old allowance back on every run.
export const PLAN_LIMITS = { free: 10, starter: 90, creator: 250, pro: 600, agency: 1300 };
const PAID_PLANS = new Set(['starter', 'creator', 'pro', 'agency']);
const LOCAL_ACTIVE = new Set(['active', 'trialing', 'past_due']);
const DODO_OK = new Set(['active', 'trialing']);

/** Comped/manual grants are intentional and must never be reconciled away. */
const isComped = (row) => String(row?.dodo_subscription_id || '').startsWith('comp:');

/**
 * Build product_id -> {plan, interval} from any env-shaped object.
 *
 * Recognises superseded products via a `_LEGACYn` suffix so grandfathered customers still resolve:
 *   DODO_PRODUCT_CREATOR_ID · DODO_PRODUCT_CREATOR_ANNUAL_ID
 *   DODO_PRODUCT_CREATOR_LEGACY_ID · DODO_PRODUCT_CREATOR_ANNUAL_LEGACY_ID
 *   DODO_PRODUCT_CREATOR_LEGACY2_ID · ... (any number of generations)
 *
 * Without the legacy ids, every customer still on old pricing would look like an unrecognised
 * product and get reported as drift — false CRITICALs on paying customers. Kept in sync with
 * buildDodoProductIndex() in src/index.js.
 */
export function buildProductMaps(env) {
  const productToPlan = {};
  const productToInterval = {};
  for (const [k, v] of Object.entries(env || {})) {
    const m = String(k).match(/^DODO_PRODUCT_([A-Z]+?)(_ANNUAL)?(_LEGACY\d*)?_ID$/);
    if (!m || !v) continue;
    const id = String(v).trim();
    if (!id) continue;
    productToPlan[id] = m[1].toLowerCase();
    productToInterval[id] = m[2] ? 'year' : 'month';
  }
  return { productToPlan, productToInterval };
}

async function listAllDodo(base, key, path) {
  const out = [];
  for (let p = 0; p < 100; p += 1) {
    const qs = new URLSearchParams({ page_size: '100', page_number: String(p) });
    const r = await fetch(`${base}${path}?${qs}`, { headers: { Authorization: `Bearer ${key}` } });
    if (!r.ok) break;
    const j = await r.json();
    const items = Array.isArray(j) ? j : j.items || j.data || [];
    out.push(...items);
    if (items.length < 100) break;
  }
  return out;
}

/**
 * @param {object} opts
 * @param {import('@supabase/supabase-js').SupabaseClient} opts.db
 * @param {string} opts.dodoKey
 * @param {string} opts.dodoBase
 * @param {Record<string,string>} opts.productToPlan
 * @param {Record<string,string>} opts.productToInterval
 * @param {boolean} [opts.apply] Write changes (default false = dry run)
 * @returns {Promise<{actions:Array, applied:number, failed:number, critical:number}>}
 */
export async function runBillingReconciliation({
  db,
  dodoKey,
  dodoBase,
  productToPlan,
  productToInterval,
  apply = false,
}) {
  const actions = [];
  const act = (sev, kind, msg, fn) => actions.push({ sev, kind, msg, fn });
  const nowIso = () => new Date().toISOString();

  const { data: subs } = await db.from('billing_subscriptions').select('*');
  // Added by a manual migration; `select('*')` simply omits it on an un-migrated schema.
  const hasProductIdColumn = Boolean(subs?.length) && 'dodo_product_id' in subs[0];
  const { data: profiles } = await db.from('profiles').select('id, plan_type');
  const dodoSubs = await listAllDodo(dodoBase, dodoKey, '/subscriptions');

  const profileById = new Map((profiles || []).map((p) => [p.id, p]));
  const dodoById = new Map(dodoSubs.map((d) => [d.subscription_id || d.id, d]));
  const subsByUser = new Map();
  for (const s of subs || []) {
    if (!subsByUser.has(s.user_id)) subsByUser.set(s.user_id, []);
    subsByUser.get(s.user_id).push(s);
  }

  // 1. Dodo is truth for anyone actively paying.
  for (const d of dodoSubs) {
    const id = d.subscription_id || d.id;
    if (!DODO_OK.has(String(d.status))) continue;
    const truePlan = productToPlan[d.product_id];
    const trueInterval = productToInterval[d.product_id] || 'month';
    const uid = d.metadata?.supabase_user_id;
    if (!truePlan || !uid) continue;

    const rows = (subsByUser.get(uid) || []).filter((r) => r.dodo_subscription_id === id);
    const active = rows.filter((r) => LOCAL_ACTIVE.has(String(r.status)));

    if (!active.length) {
      act('CRITICAL', 'paying-no-access', `${d.customer?.email} pays for ${truePlan} but has no active local row (${id})`, null);
      continue;
    }
    for (const r of active) {
      const planWrong = r.plan_type !== truePlan || r.pins_limit_per_month !== PLAN_LIMITS[truePlan];
      // billing_interval alone matters more than it looks: pinUsageBucketKey() keys the usage
      // bucket by CALENDAR MONTH for 'year' and by BILLING PERIOD for 'month'. An annual
      // subscription mislabelled 'month' would key its bucket to the period start and not
      // reset until the period ends — one year's worth of pins in a single month's allowance.
      const intervalWrong = trueInterval && r.billing_interval !== trueInterval;

      // The stored product id is what tells the app which PRICE this customer pays. A missed
      // webhook leaves it stale, which silently misprices them everywhere it is read.
      if (hasProductIdColumn && r.dodo_product_id !== d.product_id) {
        act('MED', 'product-id-drift', `${d.customer?.email}: dodo_product_id ${r.dodo_product_id || '(null)'} -> ${d.product_id}`, async () =>
          db.from('billing_subscriptions')
            .update({ dodo_product_id: d.product_id, updated_at: nowIso() })
            .eq('id', r.id)
        );
      }

      if (planWrong) {
        act('CRITICAL', 'plan-drift', `${d.customer?.email}: local ${r.plan_type}/${r.pins_limit_per_month} -> ${truePlan}/${PLAN_LIMITS[truePlan]}`, async () =>
          db.from('billing_subscriptions')
            .update({ plan_type: truePlan, pins_limit_per_month: PLAN_LIMITS[truePlan], billing_interval: trueInterval, updated_at: nowIso() })
            .eq('id', r.id)
        );
      } else if (intervalWrong) {
        act('HIGH', 'interval-drift', `${d.customer?.email}: billing_interval ${r.billing_interval} -> ${trueInterval} (wrong interval breaks monthly quota reset)`, async () =>
          db.from('billing_subscriptions')
            .update({ billing_interval: trueInterval, updated_at: nowIso() })
            .eq('id', r.id)
        );
      }
    }
    const prof = profileById.get(uid);
    if (prof && prof.plan_type !== truePlan) {
      act('HIGH', 'profile-sync', `${d.customer?.email}: profile ${prof.plan_type} -> ${truePlan}`, async () =>
        db.from('profiles').update({ plan_type: truePlan, updated_at: nowIso() }).eq('id', uid)
      );
    }
  }

  // 2. Local access with nothing paying for it.
  for (const [uid, rows] of subsByUser) {
    for (const r of rows) {
      if (!LOCAL_ACTIVE.has(String(r.status))) continue;
      if (isComped(r)) continue;
      const id = r.dodo_subscription_id;
      const d = id ? dodoById.get(id) : null;

      // Expire the row AND drop the profile in one action, so a single run leaves
      // consistent state instead of waiting for the next night.
      const expireAndDowngrade = (patch) => async () => {
        const res = await db.from('billing_subscriptions').update({ ...patch, updated_at: nowIso() }).eq('id', r.id);
        if (res.error) return res;
        const stillEntitled = (subsByUser.get(uid) || []).some(
          (o) =>
            o.id !== r.id &&
            LOCAL_ACTIVE.has(String(o.status)) &&
            (isComped(o) || DODO_OK.has(String(dodoById.get(o.dodo_subscription_id)?.status)))
        );
        if (stillEntitled) return res;
        const prof = profileById.get(uid);
        if (!prof || !PAID_PLANS.has(String(prof.plan_type || '').toLowerCase())) return res;
        return db.from('profiles').update({ plan_type: 'free', updated_at: nowIso() }).eq('id', uid);
      };

      if (!id) {
        act('HIGH', 'unbacked-row', `user ${uid.slice(0, 8)} active ${r.plan_type} row has no dodo_subscription_id -> expire + profile free`, expireAndDowngrade({ status: 'expired' }));
        continue;
      }
      if (!d || !DODO_OK.has(String(d.status))) {
        const why = d ? d.status : 'missing in Dodo';
        act('HIGH', 'access-no-payment', `user ${uid.slice(0, 8)} active ${r.plan_type} but Dodo sub is ${why} -> expire + profile free`, expireAndDowngrade({ status: 'expired', cancelled_at: nowIso() }));
        continue;
      }
      if (r.current_period_end && new Date(r.current_period_end).getTime() < Date.now()) {
        act('MED', 'stale-period', `user ${uid.slice(0, 8)} ${r.plan_type} row active but period ended ${String(r.current_period_end).slice(0, 10)}`, async () =>
          db.from('billing_subscriptions').update({ status: 'expired', updated_at: nowIso() }).eq('id', r.id)
        );
      }
    }
  }

  // 2b. Comps have no Dodo subscription, so rule 1 never sees them. Enforcement reads the
  // subscription first, but the profile drives the UI and the no-active-sub fallback.
  for (const [uid, rows] of subsByUser) {
    const live = rows.find(
      (r) =>
        isComped(r) &&
        LOCAL_ACTIVE.has(String(r.status)) &&
        (!r.current_period_end || new Date(r.current_period_end).getTime() > Date.now())
    );
    if (!live) continue;
    const prof = profileById.get(uid);
    if (!prof || prof.plan_type === live.plan_type) continue;
    act('MED', 'comp-profile-sync', `user ${uid.slice(0, 8)} comped ${live.plan_type} (${live.dodo_subscription_id}) but profile=${prof.plan_type}`, async () =>
      db.from('profiles').update({ plan_type: live.plan_type, updated_at: nowIso() }).eq('id', uid)
    );
  }

  // 3. Paid profile with nothing backing it.
  for (const p of profiles || []) {
    const pt = String(p.plan_type || 'free').toLowerCase();
    if (!PAID_PLANS.has(pt)) continue;
    const rows = subsByUser.get(p.id) || [];
    if (rows.some((r) => LOCAL_ACTIVE.has(String(r.status)))) continue;
    act('HIGH', 'ghost-paid', `profile ${p.id.slice(0, 8)} says ${pt} with no active subscription -> free`, async () =>
      db.from('profiles').update({ plan_type: 'free', updated_at: nowIso() }).eq('id', p.id)
    );
  }

  const order = { CRITICAL: 0, HIGH: 1, MED: 2, LOW: 3 };
  actions.sort((a, b) => order[a.sev] - order[b.sev]);

  let applied = 0;
  let failed = 0;
  if (apply) {
    for (const a of actions) {
      if (!a.fn) continue;
      const { error } = await a.fn();
      if (error) {
        failed += 1;
        a.error = error.message;
      } else applied += 1;
    }
  }

  // CRITICAL items with no auto-fix = a human needs to look (customer paying, no access).
  const critical = actions.filter((a) => a.sev === 'CRITICAL' && !a.fn).length;
  return { actions, applied, failed, critical };
}
