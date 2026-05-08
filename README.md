# gcp-npi-data-pipeline

This repository contains a starter pipeline for ingesting CMS/NPPES NPI data into Google Cloud Storage and Google BigQuery, with a synced provider lookup table.

## Goal
- Ingest CMS NPPES monthly full replacement files and weekly incremental files.
- Archive raw files to GCS.
- Load staged data into BigQuery.
- Maintain a `provider_lookup` table that is rebuilt from the monthly full file and updated via weekly incrementals.
- Use NPPES Version 2 as the recommended/current schema.

## Design
- `monthly` mode:
  - Download the full monthly NPI file.
  - Upload the source file to GCS.
  - Load the full file into a staging table.
  - Replace the provider lookup table with the full staging table.
- `weekly` mode:
  - Download the weekly incremental file.
  - Upload the source file to GCS.
  - Load the incremental file into a weekly staging table.
  - Merge incremental rows into the provider lookup table.

## Files
- `npi_pipeline.py`: orchestration script for download, GCS upload, BigQuery load, and lookup sync.
- `requirements.txt`: Python dependencies.
- `.gitignore`: common Python excludes.

## Usage
1. Install dependencies:

```bash
python3 -m pip install -r requirements.txt
```

2. Authenticate to Google Cloud:

```bash
gcloud auth application-default login
```

3. Run a monthly load:

```bash
python npi_pipeline.py \
  --mode monthly \
  --source-url "https://example.com/nppes_full_2026_05.zip" \
  --gcs-bucket my-bucket \
  --bq-project my-project \
  --bq-dataset my_dataset
```

4. Run a weekly incremental load:

```bash
python npi_pipeline.py \
  --mode weekly \
  --source-url "https://example.com/nppes_weekly_2026-05-01.zip" \
  --gcs-bucket my-bucket \
  --bq-project my-project \
  --bq-dataset my_dataset
```

## Notes
- CMS documentation recommends reloading the monthly full file each month.
- Weekly files should supplement the monthly full file by merging updates.
- This implementation assumes the incoming files use a pipe-delimited CSV schema similar to NPPES Version 2.
- Update the `NPI_PROVIDER_LOOKUP_SCHEMA` in `npi_pipeline.py` if your schema includes additional fields.

## Configuration
The script accepts environment variables as fallbacks:
- `BQ_PROJECT`
- `BQ_DATASET`
- `GCS_BUCKET`

## Next steps
- Add scheduler automation (Cloud Scheduler + Cloud Functions / Cloud Run).
- Add validation for schema drift and file freshness.
- Add incremental state tracking and run metadata logging.

## Cloud Run deployment scaffold
This repository now includes a Cloud Run deployment scaffold.

Files added:
- `Dockerfile` — builds a container image for Cloud Run.
- `main.py` — Flask-based HTTP entrypoint for triggering the pipeline.
- `cloudbuild.yaml` — Cloud Build configuration for building and publishing the image.

### Deploy to Cloud Run
```bash
gcloud builds submit --config cloudbuild.yaml --substitutions=_GCR_IMAGE="gcr.io/$GOOGLE_CLOUD_PROJECT/gcp-npi-data-pipeline"
```
Then deploy the container:

```bash
gcloud run deploy gcp-npi-data-pipeline \
  --image gcr.io/$GOOGLE_CLOUD_PROJECT/gcp-npi-data-pipeline:$SHORT_SHA \
  --region us-central1 \
  --platform managed \
  --allow-unauthenticated \
  --set-env-vars BQ_PROJECT=your-project,BQ_DATASET=your_dataset,GCS_BUCKET=your-bucket
```

### Cloud Run trigger
The service exposes `/run`.
Example:

```bash
curl -X POST "https://<SERVICE_URL>/run" \
  -H "Content-Type: application/json" \
  -d '{"mode":"monthly","source_url":"https://example.com/nppes_full_2026_05.zip"}'
```

### Cloud Scheduler example
Create a scheduler job for monthly full refresh:

```bash
gcloud scheduler jobs create http npi-monthly-full \
  --schedule="0 4 1 * *" \
  --uri="https://<SERVICE_URL>/run" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --time-zone="UTC" \
  --message-body='{"mode":"monthly","source_url":"https://example.com/nppes_full_2026_05.zip"}'
```

Create a scheduler job for weekly incremental merge:

```bash
gcloud scheduler jobs create http npi-weekly-incremental \
  --schedule="0 4 * * 1" \
  --uri="https://<SERVICE_URL>/run" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --time-zone="UTC" \
  --message-body='{"mode":"weekly","source_url":"https://example.com/nppes_weekly_2026-05-01.zip"}'
```

Make sure the Cloud Run service has a service account with access to BigQuery and Cloud Storage.
