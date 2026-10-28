# Uber Spend Dashboard — Apps Script web app

Online dashboard (Chart.js) querying `finances-314506.reporting.rpt_uber_spend`
live via the BigQuery Advanced Service. Results are cached 30 min in
`CacheService`.

## Deploy with clasp

```bash
npm install -g @google/clasp
clasp login

cd "Apps Scripts/Uber Dashboard"
clasp create --title "Uber Spend Dashboard" --type webapp
clasp push
clasp deploy --description "v1"
clasp open   # or grab the web app URL from Deploy > Manage deployments
```

## Or deploy from the editor

1. script.google.com → New project → paste `Code.js`, `index.html`, and
   replace the manifest with `appsscript.json` (View > Show manifest).
2. Deploy → New deployment → Web app → Execute as **Me**, access **Only myself**.

## Requirements

- The BigQuery Advanced Service is enabled via the manifest; the first run
  will prompt for the `bigquery.readonly` OAuth scope.
- Your Google account needs BigQuery read access on `finances-314506`
  (it's your project, so it does).
- Data must exist: raw tables loaded + Dataform run
  (`stg_uber_* → fct_uber_spend → rpt_uber_spend`).

## Notes

- Amounts converted to EUR in `fct_uber_spend` using monthly rates from
  `stg_exchange_rates`, with fixed fallbacks (BRL 0.18, USD 0.92, PLN 0.23).
- To share with someone else, change webapp access in `appsscript.json` and
  grant them BigQuery read access.
