#!/usr/bin/env python3

import os
import logging
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv


load_dotenv()

logger = logging.getLogger(__name__)


def _get_bq_client() -> bigquery.Client:
    project_id = os.getenv("GCP_PROJECT_ID")
    credentials_path = os.getenv("GCP_SERVICE_KEY_PATH", "service-key.json")
    try:
        if os.path.exists(credentials_path):
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )
            return bigquery.Client(credentials=credentials, project=project_id)
        return bigquery.Client(project=project_id)
    except Exception as e:
        logger.error(f"Failed to initialize BigQuery client: {e}")
        raise


def create_message_status_flat(dataset: str) -> None:
    client = _get_bq_client()
    project = client.project

    query = f"""
CREATE OR REPLACE TABLE `{project}.{dataset}.message_status_flat` AS
SELECT
  message_uuid,
  -- Count occurrences of each status
  COUNTIF(status = 'sent') AS sent,
  COUNTIF(status = 'delivered') AS delivered,
  COUNTIF(status = 'read') AS read,
  COUNTIF(status = 'failed') AS failed,
  COUNTIF(status = 'deleted') AS deleted,

  -- Latest sent status info
  ARRAY_AGG(IF(status = 'sent', timestamp, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS sent_timestamp,
  ARRAY_AGG(IF(status = 'sent', inserted_at, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS sent_inserted_at,
  ARRAY_AGG(IF(status = 'sent', updated_at, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS sent_updated_at,
  ARRAY_AGG(IF(status = 'sent', uuid, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS sent_status_uuid,
  ARRAY_AGG(IF(status = 'sent', id, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS sent_status_id,
  ARRAY_AGG(IF(status = 'sent', message_id, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS sent_status_message_id,
  ARRAY_AGG(IF(status = 'sent', number_id, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS sent_status_number_id,

  -- Latest delivered status info
  ARRAY_AGG(IF(status = 'delivered', timestamp, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS delivered_timestamp,
  ARRAY_AGG(IF(status = 'delivered', inserted_at, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS delivered_inserted_at,
  ARRAY_AGG(IF(status = 'delivered', updated_at, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS delivered_updated_at,
  ARRAY_AGG(IF(status = 'delivered', uuid, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS delivered_status_uuid,
  ARRAY_AGG(IF(status = 'delivered', id, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS delivered_status_id,
  ARRAY_AGG(IF(status = 'delivered', message_id, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS delivered_status_message_id,
  ARRAY_AGG(IF(status = 'delivered', number_id, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS delivered_status_number_id,

  -- Latest read status info
  ARRAY_AGG(IF(status = 'read', timestamp, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS read_timestamp,
  ARRAY_AGG(IF(status = 'read', inserted_at, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS read_inserted_at,
  ARRAY_AGG(IF(status = 'read', updated_at, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS read_updated_at,
  ARRAY_AGG(IF(status = 'read', uuid, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS read_status_uuid,
  ARRAY_AGG(IF(status = 'read', id, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS read_status_id,
  ARRAY_AGG(IF(status = 'read', message_id, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS read_status_message_id,
  ARRAY_AGG(IF(status = 'read', number_id, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS read_status_number_id,

  -- Latest failed status info
  ARRAY_AGG(IF(status = 'failed', timestamp, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS failed_timestamp,
  ARRAY_AGG(IF(status = 'failed', inserted_at, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS failed_inserted_at,
  ARRAY_AGG(IF(status = 'failed', updated_at, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS failed_updated_at,
  ARRAY_AGG(IF(status = 'failed', uuid, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS failed_status_uuid,
  ARRAY_AGG(IF(status = 'failed', id, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS failed_status_id,
  ARRAY_AGG(IF(status = 'failed', message_id, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS failed_status_message_id,
  ARRAY_AGG(IF(status = 'failed', number_id, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS failed_status_number_id,

  -- Latest deleted status info
  ARRAY_AGG(IF(status = 'deleted', timestamp, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS deleted_timestamp,
  ARRAY_AGG(IF(status = 'deleted', inserted_at, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS deleted_inserted_at,
  ARRAY_AGG(IF(status = 'deleted', updated_at, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS deleted_updated_at,
  ARRAY_AGG(IF(status = 'deleted', uuid, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS deleted_status_uuid,
  ARRAY_AGG(IF(status = 'deleted', id, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS deleted_status_id,
  ARRAY_AGG(IF(status = 'deleted', message_id, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS deleted_status_message_id,
  ARRAY_AGG(IF(status = 'deleted', number_id, NULL) IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS deleted_status_number_id

FROM `{project}.{dataset}.statuses_raw`
WHERE message_uuid IS NOT NULL
GROUP BY message_uuid
"""

    logger.info("Creating message_status_flat table...")
    client.query(query).result()
    logger.info("Created message_status_flat")


def create_unified_messages(dataset: str) -> None:
    client = _get_bq_client()
    project = client.project

    query = f"""
CREATE OR REPLACE TABLE `{project}.{dataset}.unified_messages` AS
SELECT
  m.id AS message_id,
  m.uuid AS message_uuid,
  m.message_type,
  m.masked_addressees,
  m.masked_author,
  m.content,
  m.author_type,
  m.direction,
  m.external_id,
  m.external_timestamp,
  m.masked_from_addr,
  m.is_deleted,
  m.rendered_content,
  m.source_type,
  m.inserted_at AS message_inserted_at,
  m.updated_at  AS message_updated_at,
  m.last_status AS msg_last_status_raw,
  m.last_status_timestamp AS msg_last_status_timestamp_raw,
  s.sent, s.delivered, s.read, s.failed, s.deleted,
  s.sent_timestamp, s.sent_inserted_at, s.sent_updated_at,
  s.sent_status_uuid, s.sent_status_id, s.sent_status_message_id, s.sent_status_number_id,
  s.delivered_timestamp, s.delivered_inserted_at, s.delivered_updated_at,
  s.delivered_status_uuid, s.delivered_status_id, s.delivered_status_message_id, s.delivered_status_number_id,
  s.read_timestamp, s.read_inserted_at, s.read_updated_at,
  s.read_status_uuid, s.read_status_id, s.read_status_message_id, s.read_status_number_id,
  s.failed_timestamp, s.failed_inserted_at, s.failed_updated_at,
  s.failed_status_uuid, s.failed_status_id, s.failed_status_message_id, s.failed_status_number_id,
  s.deleted_timestamp, s.deleted_inserted_at, s.deleted_updated_at,
  s.deleted_status_uuid, s.deleted_status_id, s.deleted_status_message_id, s.deleted_status_number_id
FROM `{project}.{dataset}.messages_raw` m
LEFT JOIN `{project}.{dataset}.message_status_flat` s
  ON m.uuid = s.message_uuid
"""

    logger.info("Creating unified_messages table...")
    client.query(query).result()
    logger.info("Created unified_messages")


def create_data_quality_tables(dataset: str) -> None:
    client = _get_bq_client()
    project = client.project

    # Check for duplicate messages
    dq_duplicates = f"""
CREATE OR REPLACE TABLE `{project}.{dataset}.dq_duplicate_messages` AS
SELECT id AS message_id, COUNT(*) AS dup_count
FROM `{project}.{dataset}.messages_raw`
GROUP BY id
HAVING COUNT(*) > 1
"""

    # Check for missing required fields
    dq_missing = f"""
CREATE OR REPLACE TABLE `{project}.{dataset}.dq_missing_required_fields` AS
SELECT *
FROM `{project}.{dataset}.messages_raw`
WHERE id IS NULL OR uuid IS NULL OR inserted_at IS NULL
"""

    logger.info("Creating data quality tables...")
    for name, query in [
        ("dq_duplicate_messages", dq_duplicates),
        ("dq_missing_required_fields", dq_missing),
    ]:
        logger.info(f"Creating {name}")
        client.query(query).result()
    logger.info("Data quality tables created")


def run_sql_transforms(dataset: str = None) -> None:
    dataset = dataset or os.getenv("BIGQUERY_DATASET", "whatsapp_healthcare")
    create_message_status_flat(dataset)
    create_unified_messages(dataset)
    create_data_quality_tables(dataset)



