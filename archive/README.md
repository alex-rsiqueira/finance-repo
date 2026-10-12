# Archive

Code that no longer runs, kept for reference. Nothing here is deployed, linted or tested.

| Path | What it was | Stopped |
|---|---|---|
| `atualiza-dados-nubank/` | Cloud Function (1st gen) that loaded Nubank into `raw.tb_nubank_*` through the unofficial `pynubank` client, run daily at 08:00 by Cloud Scheduler. | Last load 2024-03-14; by 2026-10 every run failed with HTTP 403 from Nubank. Retired 2026-10, once the Pluggy ingestion (`GCP/cloud-function/atualiza-dados-pluggy`) took over Nubank. |
| `cloudbuild.yaml` | Cloud Build config that deployed `atualiza-dados-nubank` from `main`. | Retired with the function. |
| `nubankpy to Bigquery.ipynb` | Notebook the function was developed from. | Retired with the function. |

The `raw.tb_nubank_*` tables and the Dataform models that read them are unchanged.
