import os
from argparse import Namespace
from flask import Flask, request, jsonify

from npi_pipeline import run_pipeline, DEFAULT_BQ_DATASET, DEFAULT_GCS_PREFIX, DEFAULT_LOOKUP_TABLE, DEFAULT_STAGING_FULL_TABLE, DEFAULT_STAGING_WEEKLY_TABLE

app = Flask(__name__)


def parse_request_args():
    body = request.get_json(silent=True) or {}
    def get(name, default=None):
        return body.get(name, request.args.get(name, default))

    skip_gcs_upload = get("skip_gcs_upload", False)
    if isinstance(skip_gcs_upload, str):
        skip_gcs_upload = skip_gcs_upload.lower() in {"1", "true", "yes", "on"}

    return Namespace(
        mode=get("mode"),
        source_url=get("source_url"),
        source_file=get("source_file"),
        gcs_bucket=get("gcs_bucket", os.environ.get("GCS_BUCKET")),
        gcs_prefix=get("gcs_prefix", os.environ.get("GCS_PREFIX", DEFAULT_GCS_PREFIX)),
        bq_project=get("bq_project", os.environ.get("BQ_PROJECT")),
        bq_dataset=get("bq_dataset", os.environ.get("BQ_DATASET", DEFAULT_BQ_DATASET)),
        staging_full_table=get("staging_full_table", os.environ.get("STAGING_FULL_TABLE", DEFAULT_STAGING_FULL_TABLE)),
        staging_weekly_table=get("staging_weekly_table", os.environ.get("STAGING_WEEKLY_TABLE", DEFAULT_STAGING_WEEKLY_TABLE)),
        lookup_table=get("lookup_table", os.environ.get("LOOKUP_TABLE", DEFAULT_LOOKUP_TABLE)),
        run_date=get("run_date"),
        skip_gcs_upload=skip_gcs_upload,
    )


@app.route("/", methods=["GET"])
def health_check():
    return jsonify({"status": "ok", "service": "npi_pipeline"})


@app.route("/run", methods=["POST", "GET"])
def run():
    args = parse_request_args()
    if not args.mode:
        return jsonify({"error": "Missing required parameter: mode"}), 400
    if not args.source_url and not args.source_file:
        return jsonify({"error": "Missing required parameter: source_url or source_file"}), 400

    try:
        run_pipeline(args)
        return jsonify({"status": "success", "mode": args.mode}), 200
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
