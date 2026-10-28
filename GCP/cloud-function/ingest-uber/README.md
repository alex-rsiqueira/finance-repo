# ingest-uber

Loads Uber data exports into BigQuery (finance-backend `plans/uber-ingestion.md`).

```
Gastos/Uber/inbox/ (iCloud) --Mac job, GCP/local-sync/--> gs://finance_uber_data/inbox/<zip>
    --object finalized--> ingest-uber --> raw.tb_uber_trip_rows
                                          raw.tb_uber_eats_order_rows
                                          raw.tb_uber_eats_order_detail_rows
                                          raw.tb_ingestion_log
                          zip moved to gs://finance_uber_data/archive/<export date>/
                          then a Dataform run of the `uber` tag
```

Every CSV row of the trips, Eats orders and Eats order details files is stored as received, as
one JSON `record`; Dataform's staging picks the fields (so a renamed Uber column needs a
staging change, never a new table). How a run goes is in `main.py`'s docstring.

## Deploy

`.github/workflows/deploy-uber.yml` deploys the Cloud Run function on every merge to `main` that
touches this folder.

## One-time setup (after the first deploy)

```bash
PROJECT=finances-314506
SA=16973409618-compute@developer.gserviceaccount.com

# The function reads, copies and deletes objects in the Uber bucket only.
gcloud storage buckets add-iam-policy-binding gs://finance_uber_data \
  --member="serviceAccount:$SA" --role=roles/storage.objectAdmin

# Cloud Storage publishes its events through Pub/Sub.
GCS_AGENT="$(gcloud storage service-agent --project=$PROJECT)"
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:$GCS_AGENT" --role=roles/pubsub.publisher

# The trigger's account receives events and may call the (private) service.
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:$SA" --role=roles/eventarc.eventReceiver
gcloud run services add-iam-policy-binding ingest-uber --region=us-central1 --project=$PROJECT \
  --member="serviceAccount:$SA" --role=roles/run.invoker

gcloud eventarc triggers create ingest-uber --project=$PROJECT --location=us-central1 \
  --destination-run-service=ingest-uber --destination-run-region=us-central1 \
  --event-filters="type=google.cloud.storage.object.v1.finalized" \
  --event-filters="bucket=finance_uber_data" \
  --service-account="$SA"
```

## Running a zip again

A failed run is logged in `raw.tb_ingestion_log` (`source_id = 'uber'`, `status = 'error'`) and
leaves the zip in `inbox/`. Moving it to a new name there runs it again:

```bash
gcloud storage mv "gs://finance_uber_data/inbox/<zip>" "gs://finance_uber_data/inbox/<zip, renamed>"
```

## Monitoring

```sql
SELECT processed_at, object_name, status, rows_in_file, rows_loaded, error_message
FROM `finances-314506.raw.tb_ingestion_log`
WHERE source_id = 'uber'
ORDER BY processed_at DESC;
```

## Tests

```bash
pip install -r requirements.txt pytest
python -m pytest -q tests
```
