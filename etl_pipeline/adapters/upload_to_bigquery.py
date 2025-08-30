#!/usr/bin/env python3

import pandas as pd
import os
from pathlib import Path
import logging

from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class BigQueryLoader:
    """Simple BigQuery uploader"""

    def __init__(
        self,
        project_id: str = None,
        dataset_id: str = None,
        credentials_path: str = None,
    ):
        self.project_id = project_id or os.getenv("GCP_PROJECT_ID", "your-project-id")
        self.dataset_id = dataset_id or os.getenv(
            "BIGQUERY_DATASET", "whatsapp_healthcare"
        )
        self.credentials_path = credentials_path or os.getenv(
            "GCP_SERVICE_KEY_PATH", "../service-key.json"
        )
        self.location = os.getenv("BIGQUERY_LOCATION", "US")
        self.client = None

        try:
            credentials_path_obj = Path(self.credentials_path)
            if not credentials_path_obj.is_absolute():
                if self.credentials_path.startswith("../"):
                    credentials_path_obj = Path.cwd().parent / self.credentials_path[3:]
                else:
                    credentials_path_obj = Path.cwd() / credentials_path_obj

            if credentials_path_obj.exists():
                credentials = service_account.Credentials.from_service_account_file(
                    str(credentials_path_obj)
                )
                self.client = bigquery.Client(
                    credentials=credentials, project=self.project_id
                )
                logger.info(
                    f"BigQuery client initialized for project: {self.project_id}"
                )
            else:
                logger.warning(f"Credentials file not found at {credentials_path_obj}")
                self.client = bigquery.Client(project=self.project_id)
                logger.info(
                    f"BigQuery client initialized with default credentials for project: {self.project_id}"
                )
        except Exception as e:
            logger.warning(f"Failed to initialize BigQuery client: {e}")
            logger.info("BigQuery upload will be skipped")
            self.client = None

    def is_available(self) -> bool:
        """Check if BigQuery is available"""
        return self.client is not None

    def create_dataset_if_not_exists(self) -> None:
        """Create dataset if it doesn't exist"""
        if not self.is_available():
            logger.warning("BigQuery client not available. Skipping dataset creation")
            return

        dataset_ref = self.client.dataset(self.dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
            logger.info(f"Dataset {self.dataset_id} already exists")
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self.location
            self.client.create_dataset(dataset)
            logger.info(
                f"Created dataset {self.dataset_id} in location {self.location}"
            )

    def load_dataframe(
        self, df: pd.DataFrame, table_name: str, if_exists: str = "replace"
    ) -> None:
        """Upload pandas DataFrame to BigQuery"""
        if not self.is_available():
            logger.warning(
                f"BigQuery client not available. Skipping upload to {table_name}"
            )
            return

        logger.info(f"Uploading {len(df)} rows to {self.dataset_id}.{table_name}")

        table_ref = f"{self.project_id}.{self.dataset_id}.{table_name}"

        job_config = bigquery.LoadJobConfig()
        if if_exists == "replace":
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        elif if_exists == "append":
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        job_config.autodetect = True

        job = self.client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
        job.result()  # Wait for completion

        logger.info(f"Successfully uploaded data to {table_ref}")

    def load_table(self, df: pd.DataFrame, table_name: str, if_exists: str = "replace") -> bool:
        """Upload a single table to BigQuery"""
        try:
            self.load_dataframe(df, table_name, if_exists)
            return True
        except Exception as e:
            logger.error(f"Failed to upload {table_name}: {e}")
            return False

    def ensure_dataset_exists(self) -> None:
        """Ensure the dataset exists, create if needed"""
        self.create_dataset_if_not_exists()
