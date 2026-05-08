import argparse
import logging
import os
import tempfile
import zipfile
from datetime import datetime

import requests
from google.cloud import bigquery, storage
from google.cloud.bigquery import SchemaField

CSV_DELIMITER = "|"
DEFAULT_GCS_PREFIX = "npi"
DEFAULT_BQ_DATASET = "npi_dataset"
DEFAULT_STAGING_FULL_TABLE = "cms_npi_full_stage"
DEFAULT_STAGING_WEEKLY_TABLE = "cms_npi_weekly_stage"
DEFAULT_LOOKUP_TABLE = "provider_lookup"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

NPI_PROVIDER_LOOKUP_SCHEMA = [
    SchemaField("npi", "STRING", mode="REQUIRED"),
    SchemaField("entity_type_code", "STRING"),
    SchemaField("replacement_npi", "STRING"),
    SchemaField("employer_identification_number", "STRING"),
    SchemaField("provider_organization_name_legal_business_name", "STRING"),
    SchemaField("provider_last_name_legal_name", "STRING"),
    SchemaField("provider_first_name", "STRING"),
    SchemaField("provider_middle_name", "STRING"),
    SchemaField("provider_name_prefix_text", "STRING"),
    SchemaField("provider_name_suffix_text", "STRING"),
    SchemaField("provider_credential_text", "STRING"),
    SchemaField("provider_other_credential_text", "STRING"),
    SchemaField("provider_first_line_business_mailing_address", "STRING"),
    SchemaField("provider_business_mailing_address_city_name", "STRING"),
    SchemaField("provider_business_mailing_address_state_name", "STRING"),
    SchemaField("provider_business_mailing_address_postal_code", "STRING"),
    SchemaField("provider_business_mailing_address_country_code", "STRING"),
    SchemaField("provider_business_mailing_address_telephone_number", "STRING"),
    SchemaField("provider_business_mailing_address_fax_number", "STRING"),
    SchemaField("provider_enumeration_date", "DATE"),
    SchemaField("last_update_date", "DATE"),
    SchemaField("provider_enumeration_status", "STRING"),
    SchemaField("provider_enumeration_type_code", "STRING"),
    SchemaField("authorization_date", "DATE"),
    SchemaField("authorization_license_number", "STRING"),
    SchemaField("authorization_state_code", "STRING"),
    SchemaField("has_sole_proprietor", "STRING"),
    SchemaField("is_organization_subpart", "STRING"),
    SchemaField("other_provider_identifier", "STRING"),
    SchemaField("provider_taxonomy_code_1", "STRING"),
    SchemaField("provider_taxonomy_code_2", "STRING"),
    SchemaField("provider_taxonomy_code_3", "STRING"),
    SchemaField("provider_taxonomy_code_4", "STRING"),
    SchemaField("provider_taxonomy_code_5", "STRING"),
    SchemaField("provider_license_number_1", "STRING"),
    SchemaField("provider_license_number_state_code_1", "STRING"),
    SchemaField("provider_license_number_2", "STRING"),
    SchemaField("provider_license_number_state_code_2", "STRING"),
    SchemaField("provider_business_practice_location_address_city_name", "STRING"),
    SchemaField("provider_business_practice_location_address_state_name", "STRING"),
    SchemaField("provider_business_practice_location_address_postal_code", "STRING"),
    SchemaField("provider_business_practice_location_address_country_code", "STRING"),
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="CMS/NPPES NPI ingestion pipeline: download source, archive to GCS, load to BigQuery, and keep a synced provider lookup table."
    )
    parser.add_argument("--mode", choices=["monthly", "weekly"], required=True,
                        help="Run mode. monthly loads a full replacement file, weekly merges incremental rows.")
    parser.add_argument("--source-url", help="CMS source URL for the NPI file. Use a full monthly or weekly file URL.")
    parser.add_argument("--source-file", help="Local source file path instead of downloading from URL.")
    parser.add_argument("--gcs-bucket", help="GCS bucket for raw/archive and staging CSV files.")
    parser.add_argument("--gcs-prefix", default=DEFAULT_GCS_PREFIX,
                        help="GCS prefix for raw file organization.")
    parser.add_argument("--bq-project", help="BigQuery project to use.")
    parser.add_argument("--bq-dataset", default=DEFAULT_BQ_DATASET,
                        help="BigQuery dataset for staging and lookup tables.")
    parser.add_argument("--staging-full-table", default=DEFAULT_STAGING_FULL_TABLE,
                        help="BigQuery staging table name for monthly full loads.")
    parser.add_argument("--staging-weekly-table", default=DEFAULT_STAGING_WEEKLY_TABLE,
                        help="BigQuery staging table name for weekly incremental loads.")
    parser.add_argument("--lookup-table", default=DEFAULT_LOOKUP_TABLE,
                        help="BigQuery provider lookup table name.")
    parser.add_argument("--run-date", help="Optional run date in YYYY-MM-DD format. Defaults to today.")
    parser.add_argument("--skip-gcs-upload", action="store_true",
                        help="Skip uploading the source file to GCS and only perform BigQuery load from a local CSV file.")
    return parser.parse_args()


def require_value(value, name):
    if not value:
        raise ValueError(f"Missing required configuration: {name}")
    return value


def download_source(source_url: str, destination_dir: str) -> str:
    local_path = os.path.join(destination_dir, os.path.basename(source_url.split("?")[0]))
    logger.info("Downloading source file from %s", source_url)
    response = requests.get(source_url, stream=True, timeout=120)
    response.raise_for_status()
    with open(local_path, "wb") as output_file:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                output_file.write(chunk)
    logger.info("Downloaded source file to %s", local_path)
    return local_path


def extract_first_csv(zip_path: str, destination_dir: str) -> str:
    if not zipfile.is_zipfile(zip_path):
        raise ValueError("Expected a ZIP archive for extraction, but the file is not valid ZIP format.")

    with zipfile.ZipFile(zip_path, "r") as archive:
        csv_files = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if not csv_files:
            raise ValueError("No CSV file found inside the ZIP archive.")
        extracted_name = csv_files[0]
        extracted_path = archive.extract(extracted_name, destination_dir)
        logger.info("Extracted CSV %s from %s", extracted_name, zip_path)
        return extracted_path


def upload_file_to_gcs(bucket_name: str, source_path: str, destination_blob_name: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    logger.info("Uploading %s to gs://%s/%s", source_path, bucket_name, destination_blob_name)
    blob.upload_from_filename(source_path)
    logger.info("Uploaded file to gs://%s/%s", bucket_name, destination_blob_name)
    return f"gs://{bucket_name}/{destination_blob_name}"


def load_csv_to_bigquery(project: str, dataset: str, table_name: str, gcs_uri: str, schema):
    bq_client = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        field_delimiter=CSV_DELIMITER,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        allow_quoted_newlines=True,
        autodetect=False,
    )
    logger.info("Loading CSV from %s into BigQuery table %s", gcs_uri, table_id)
    load_job = bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()
    table = bq_client.get_table(table_id)
    logger.info("Loaded %d rows into %s", table.num_rows, table_id)
    return table


def copy_table_to_lookup(project: str, dataset: str, source_table: str, destination_table: str):
    client = bigquery.Client(project=project)
    source_ref = f"{project}.{dataset}.{source_table}"
    destination_ref = f"{project}.{dataset}.{destination_table}"
    job_config = bigquery.CopyJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    logger.info("Replacing lookup table %s from source staging table %s", destination_ref, source_ref)
    copy_job = client.copy_table(source_ref, destination_ref, job_config=job_config)
    copy_job.result()
    logger.info("Lookup table %s replaced successfully", destination_ref)
    return client.get_table(destination_ref)


def ensure_table_exists(project: str, dataset: str, table_name: str, schema):
    client = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.{table_name}"
    try:
        return client.get_table(table_id)
    except Exception as ex:
        logger.info("Lookup table %s not found, creating it with schema", table_id)
        table = bigquery.Table(table_id, schema=schema)
        return client.create_table(table)


def build_merge_query(project: str, dataset: str, staging_table: str, lookup_table: str, schema):
    column_names = [field.name for field in schema]
    key = "npi"
    update_fields = [name for name in column_names if name != key]
    update_assignments = ",\n        ".join([f"T.{name} = S.{name}" for name in update_fields])
    insert_columns = ", ".join(column_names)
    insert_values = ", ".join([f"S.{name}" for name in column_names])
    return f"""
MERGE `{project}.{dataset}.{lookup_table}` T
USING `{project}.{dataset}.{staging_table}` S
ON T.{key} = S.{key}
WHEN MATCHED THEN
  UPDATE SET
    {update_assignments}
WHEN NOT MATCHED THEN
  INSERT ({insert_columns})
  VALUES ({insert_values})
"""


def merge_staging_to_lookup(project: str, dataset: str, staging_table: str, lookup_table: str, schema):
    client = bigquery.Client(project=project)
    ensure_table_exists(project, dataset, lookup_table, schema)
    merge_sql = build_merge_query(project, dataset, staging_table, lookup_table, schema)
    logger.info("Merging incremental data from %s into %s", staging_table, lookup_table)
    query_job = client.query(merge_sql)
    query_job.result()
    table = client.get_table(f"{project}.{dataset}.{lookup_table}")
    logger.info("Merge complete. Final lookup table row count: %d", table.num_rows)
    return table


def build_gcs_blob_name(prefix: str, mode: str, run_date: datetime, file_name: str) -> str:
    date_path = run_date.strftime("%Y%m%d")
    clean_name = os.path.basename(file_name)
    return f"{prefix}/raw/{mode}/{date_path}/{clean_name}"


def run_pipeline(args):
    run_date = datetime.strptime(args.run_date, "%Y-%m-%d").date() if args.run_date else datetime.utcnow().date()
    project = args.bq_project or os.environ.get("BQ_PROJECT")
    dataset = args.bq_dataset or os.environ.get("BQ_DATASET")
    bucket = args.gcs_bucket or os.environ.get("GCS_BUCKET")
    source_url = args.source_url
    source_file = args.source_file

    require_value(project, "BQ_PROJECT")
    require_value(dataset, "BQ_DATASET")
    require_value(bucket, "GCS_BUCKET")
    if not source_url and not source_file:
        raise ValueError("Either --source-url or --source-file must be provided.")

    if source_url and source_file:
        raise ValueError("Only one of --source-url or --source-file should be provided.")

    with tempfile.TemporaryDirectory() as tempdir:
        if source_url:
            local_source = download_source(source_url, tempdir)
        else:
            local_source = os.path.abspath(source_file)
            if not os.path.exists(local_source):
                raise ValueError(f"Local source file does not exist: {local_source}")

        if args.skip_gcs_upload:
            logger.info("Skipping GCS upload because --skip-gcs-upload was requested.")
            if local_source.lower().endswith(".zip"):
                local_source = extract_first_csv(local_source, tempdir)
            staging_gcs_uri = None
        else:
            raw_blob_name = build_gcs_blob_name(args.gcs_prefix, args.mode, run_date, local_source)
            raw_gcs_uri = upload_file_to_gcs(bucket, local_source, raw_blob_name)
            if local_source.lower().endswith(".zip"):
                extracted_csv = extract_first_csv(local_source, tempdir)
                extracted_blob_name = raw_blob_name.rsplit(".", 1)[0] + ".csv"
                staging_csv_uri = upload_file_to_gcs(bucket, extracted_csv, extracted_blob_name)
                staging_gcs_uri = staging_csv_uri
            else:
                staging_gcs_uri = raw_gcs_uri

        if args.mode == "monthly":
            staging_table = args.staging_full_table
            lookup_table = args.lookup_table
            if not staging_gcs_uri:
                raise ValueError("GCS staging URI is required for monthly mode.")
            load_csv_to_bigquery(project, dataset, staging_table, staging_gcs_uri, NPI_PROVIDER_LOOKUP_SCHEMA)
            copy_table_to_lookup(project, dataset, staging_table, lookup_table)
        else:
            staging_table = args.staging_weekly_table
            lookup_table = args.lookup_table
            if not staging_gcs_uri:
                raise ValueError("GCS staging URI is required for weekly mode.")
            load_csv_to_bigquery(project, dataset, staging_table, staging_gcs_uri, NPI_PROVIDER_LOOKUP_SCHEMA)
            merge_staging_to_lookup(project, dataset, staging_table, lookup_table, NPI_PROVIDER_LOOKUP_SCHEMA)

        logger.info("Pipeline run completed for mode=%s date=%s", args.mode, run_date)


if __name__ == "__main__":
    run_pipeline(parse_args())
