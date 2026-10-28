/**
 * Uber Spend Dashboard — Apps Script web app.
 * Serves index.html; exposes per-tab data functions querying the reporting
 * layer via the BigQuery Advanced Service. Detail tabs use compact field
 * names to keep google.script.run payloads small.
 */

const PROJECT_ID = 'finances-314506';
const RPT = 'finances-314506.reporting';
const CACHE_TTL_SECONDS = 1800; // 30 min

function doGet() {
  return HtmlService.createHtmlOutputFromFile('index')
    .setTitle('Uber Spend Dashboard')
    .addMetaTag('viewport', 'width=device-width, initial-scale=1');
}

/**
 * Runs a query and returns rows as an array of objects.
 * @param {string} sql
 * @return {Object[]}
 */
function runQuery_(sql) {
  const request = { query: sql, useLegacySql: false };
  let queryResults = BigQuery.Jobs.query(request, PROJECT_ID);
  const jobId = queryResults.jobReference.jobId;

  while (!queryResults.jobComplete) {
    Utilities.sleep(500);
    queryResults = BigQuery.Jobs.getQueryResults(PROJECT_ID, jobId);
  }

  let rows = queryResults.rows || [];
  while (queryResults.pageToken) {
    queryResults = BigQuery.Jobs.getQueryResults(PROJECT_ID, jobId, {
      pageToken: queryResults.pageToken,
    });
    rows = rows.concat(queryResults.rows || []);
  }

  const fields = queryResults.schema.fields;
  return rows.map(function (r) {
    const obj = {};
    fields.forEach(function (f, i) {
      let v = r.f[i].v;
      if (v !== null && (f.type === 'FLOAT' || f.type === 'INTEGER' || f.type === 'NUMERIC')) {
        v = Number(v);
      }
      obj[f.name] = v;
    });
    return obj;
  });
}

/**
 * Cache wrapper; silently skips caching when the payload exceeds the
 * CacheService 100KB value limit.
 */
function cached_(key, forceRefresh, producer) {
  const cache = CacheService.getScriptCache();
  if (forceRefresh) cache.remove(key);
  const hit = forceRefresh ? null : cache.get(key);
  if (hit) return JSON.parse(hit);
  const data = producer();
  try {
    cache.put(key, JSON.stringify(data), CACHE_TTL_SECONDS);
  } catch (e) { /* payload too large for cache — serve uncached */ }
  return data;
}

/** Overview tab: monthly grain by type/city/detail. */
function getDashboardData(forceRefresh) {
  return cached_('dashboard_data_v2', forceRefresh, function () {
    return {
      rows: runQuery_(
        'SELECT CAST(spend_month AS STRING) AS month, spend_type, city, detail, ' +
        '  ROUND(SUM(amount_eur), 2) AS amount_eur, SUM(transactions) AS transactions, ' +
        '  ROUND(SUM(total_distance_km), 1) AS distance_km ' +
        'FROM `' + RPT + '.rpt_uber_spend` ' +
        'GROUP BY month, spend_type, city, detail ORDER BY month'
      ),
    };
  });
}

/** Rides tab: trip-level detail. Compact keys: see index.html RIDE_COLS. */
function getRidesData(forceRefresh) {
  return cached_('rides_data_v1', forceRefresh, function () {
    return {
      rows: runQuery_(
        'SELECT CAST(trip_date AS STRING) AS d, city, product_type AS p, ' +
        '  begintrip_address AS fa, begintrip_lat AS fy, begintrip_lng AS fx, ' +
        '  dropoff_address AS ta, dropoff_lat AS ty, dropoff_lng AS tx, ' +
        '  distance_km AS km, trip_duration_min AS mn, ' +
        '  fare_currency AS cur, amount_local AS amt, amount_eur AS eur ' +
        'FROM `' + RPT + '.rpt_uber_rides_detail` ' +
        'ORDER BY request_time DESC'
      ),
    };
  });
}

/** Eats tab: item-level detail. Compact keys: see index.html EATS_COLS. */
function getEatsData(forceRefresh) {
  return cached_('eats_data_v1', forceRefresh, function () {
    return {
      rows: runQuery_(
        'SELECT CAST(order_date AS STRING) AS d, city, restaurant_name AS rest, ' +
        '  establishment_type AS typ, CAST(order_key AS STRING) AS ok, ' +
        '  item_name AS item, item_quantity AS qty, ' +
        '  currency AS cur, item_amount_eur AS ieur, ' +
        '  order_price_eur AS oeur ' +
        'FROM `' + RPT + '.rpt_uber_eats_items` ' +
        'ORDER BY request_time DESC'
      ),
    };
  });
}
