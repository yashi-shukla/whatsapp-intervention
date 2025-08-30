#!/usr/bin/env python3

import logging
import time
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

log_level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper())
logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def save_results_locally(results: dict, output_dir: str = "output") -> None:
    """Save ETL results to local JSONL files as fallback when BigQuery fails.

    Args:
        results: Dictionary containing ETL results with keys:
            - unified_messages: DataFrame or list of UnifiedMessage objects
            - duplicates: List of DuplicateRecord objects
            - quality_report: DataQualityReport object
        output_dir: Directory path to save files (relative to etl_pipeline/)
    """
    from pathlib import Path
    from models import save_jsonl

    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    logger.info(f"Saving results to {output_path}")

    # Save unified messages
    unified_file = output_path / "unified_messages.jsonl"
    if (results.get("unified_messages") is not None and
        not getattr(results["unified_messages"], 'empty', False)):
        save_jsonl(results["unified_messages"], str(unified_file))
        logger.info(f"Saved unified messages to {unified_file}")

    # Save duplicates
    duplicates_file = output_path / "duplicates.jsonl"
    if results.get("duplicates"):
        save_jsonl(results["duplicates"], str(duplicates_file))
        logger.info(f"Saved duplicates to {duplicates_file}")

    # Save quality report
    quality_file = output_path / "quality_report.json"
    if results.get("quality_report"):
        with open(quality_file, "w", encoding="utf-8") as f:
            f.write(
                results["quality_report"].model_dump_json(
                    indent=2, exclude_unset=True
                )
            )
        logger.info(f"Saved quality report to {quality_file}")

    logger.info("Local save completed successfully")


def run_etl_pipeline():
    """Run the ETL pipeline"""
    logger.info("Starting ETL pipeline")

    start_time = time.time()

    try:
        logger.info("Extracting data...")
        from adapters.extract_data import GSheetsExtractor

        extractor = GSheetsExtractor()
        messages_df, statuses_df = extractor.extract_all()

        logger.info("Transforming data...")
        from transform.transform_data import DataTransformer

        transformer = DataTransformer(messages_df, statuses_df)
        results = transformer.transform_all()

        # Upload to BigQuery (optional)
        enable_bigquery = os.getenv("ENABLE_BIGQUERY_UPLOAD", "true").lower() == "true"

        if not enable_bigquery:
            logger.info("BigQuery upload disabled by configuration. Skipping...")
            save_results_locally(results)
        else:
            logger.info("Uploading to BigQuery...")
            try:
                from adapters.upload_to_bigquery import BigQueryLoader

                loader = BigQueryLoader()

                if not loader.is_available():
                    logger.warning("BigQuery client not available")
                    raise Exception("BigQuery client not initialized")

                # Prepare tables for upload
                tables_to_upload = {
                    "unified_messages": results["unified_messages"],
                    "duplicates": pd.DataFrame(
                        [dup.model_dump() for dup in results["duplicates"]]
                    ),
                    "messages_raw": messages_df,
                    "statuses_raw": statuses_df,
                }

                # Add status flat table if available
                status_flat_df = getattr(transformer, "status_flat", None)
                if status_flat_df is not None:
                    tables_to_upload["message_status_flat"] = status_flat_df

                # Ensure dataset exists
                loader.ensure_dataset_exists()

                # Upload each table
                upload_success_count = 0
                for table_name, df in tables_to_upload.items():
                    if loader.load_table(df, table_name, if_exists="replace"):
                        upload_success_count += 1

                logger.info(f"Successfully uploaded {upload_success_count}/{len(tables_to_upload)} tables to BigQuery")

                # Run BigQuery transformations
                logger.info("Running SQL transformations in BigQuery...")
                from transform.sql_transform import run_sql_transforms

                run_sql_transforms(os.getenv("BIGQUERY_DATASET", "whatsapp_healthcare"))
                logger.info("SQL transformations completed")

                logger.info("BigQuery upload completed")

            except Exception as e:
                logger.warning(f"BigQuery failed: {e}")
                save_results_locally(results)

        # Summary
        elapsed_time = time.time() - start_time
        logger.info(f"ETL Pipeline completed in {elapsed_time:.2f} seconds")

        return results

    except Exception as e:
        logger.error(f"ETL Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    run_etl_pipeline()
