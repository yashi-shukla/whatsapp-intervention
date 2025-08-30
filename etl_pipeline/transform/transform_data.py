#!/usr/bin/env python3

import pandas as pd
import logging
from typing import Dict, Any, List
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

from models import (
    DuplicateRecord,
    QualityCheckResult,
    DataQualityReport,
)


class DataTransformer:
    """Transform WhatsApp messaging data according to project requirements"""

    def __init__(self, messages_df: pd.DataFrame, statuses_df: pd.DataFrame):
        self.messages_df = messages_df.copy()
        self.statuses_df = statuses_df.copy()

    def clean_data(self) -> None:
        """Basic data cleaning"""
        logger.info("Cleaning data...")

        # Clean messages data
        if not self.messages_df.empty and "id" in self.messages_df.columns:
            self.messages_df = self.messages_df.dropna(subset=["id", "inserted_at"])

        # Clean statuses data (only if it has data and expected columns)
        if (not self.statuses_df.empty and
            all(col in self.statuses_df.columns for col in ["message_uuid", "status", "timestamp"])):
            self.statuses_df = self.statuses_df.dropna(
                subset=["message_uuid", "status", "timestamp"]
            )

        # Convert date columns
        date_columns = ["inserted_at", "updated_at", "timestamp", "external_timestamp"]
        for df_name, df in [
            ("messages", self.messages_df),
            ("statuses", self.statuses_df),
        ]:
            if not df.empty:
                for col in date_columns:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors="coerce")

        logger.info(
            f"Cleaned: {len(self.messages_df)} messages, {len(self.statuses_df)} statuses"
        )

    def transform_unified_view(self) -> pd.DataFrame:
        """Transform statuses to wide format and join with messages.

        Creates one row per message with status counts and latest status info.
        """
        logger.info("Transforming data...")

        # Build status table
        if self.statuses_df.empty:
            logger.info("Statuses table is empty; proceeding with message fields only")
            status_wide = pd.DataFrame(
                columns=[
                    "message_uuid",
                    # counts
                    "sent",
                    "delivered",
                    "read",
                    "failed",
                    "deleted",
                    # sent
                    "sent_timestamp",
                    "sent_inserted_at",
                    "sent_updated_at",
                    "sent_status_uuid",
                    "sent_status_id",
                    "sent_status_message_id",
                    "sent_status_number_id",
                    # delivered
                    "delivered_timestamp",
                    "delivered_inserted_at",
                    "delivered_updated_at",
                    "delivered_status_uuid",
                    "delivered_status_id",
                    "delivered_status_message_id",
                    "delivered_status_number_id",
                    # read
                    "read_timestamp",
                    "read_inserted_at",
                    "read_updated_at",
                    "read_status_uuid",
                    "read_status_id",
                    "read_status_message_id",
                    "read_status_number_id",
                    # failed
                    "failed_timestamp",
                    "failed_inserted_at",
                    "failed_updated_at",
                    "failed_status_uuid",
                    "failed_status_id",
                    "failed_status_message_id",
                    "failed_status_number_id",
                    # deleted
                    "deleted_timestamp",
                    "deleted_inserted_at",
                    "deleted_updated_at",
                    "deleted_status_uuid",
                    "deleted_status_id",
                    "deleted_status_message_id",
                    "deleted_status_number_id",
                ]
            )
        else:
            statuses = self.statuses_df.copy()
            for col in ["timestamp", "inserted_at", "updated_at"]:
                if col in statuses.columns:
                    statuses[col] = pd.to_datetime(statuses[col], errors="coerce")

            statuses_subset = statuses[
                [
                    c
                    for c in [
                        "message_uuid",
                        "status",
                        "timestamp",
                        "uuid",
                        "id",
                        "message_id",
                        "number_id",
                        "inserted_at",
                        "updated_at",
                    ]
                    if c in statuses.columns
                ]
            ].copy()

            # Pivot counts for sent/delivered/read
            pivot_statuses = ["sent", "delivered", "read", "failed", "deleted"]
            counts_df = (
                statuses_subset[statuses_subset["status"].isin(pivot_statuses)]
                .groupby(["message_uuid", "status"])
                .size()
                .unstack(fill_value=0)
            )
            for c in pivot_statuses:
                if c not in counts_df.columns:
                    counts_df[c] = 0
            for c in pivot_statuses:
                counts_df[c] = counts_df[c].astype("int64")
            counts_df = counts_df.reset_index()

            def get_latest_status(status_type: str, prefix: str) -> pd.DataFrame:
                df = statuses_subset[statuses_subset["status"] == status_type].copy()
                if df.empty:
                    return pd.DataFrame(columns=["message_uuid", f"{prefix}_timestamp"])

                # Get latest by timestamp
                latest = df.sort_values("timestamp").groupby("message_uuid").last().reset_index()
                latest = latest[["message_uuid", "timestamp", "uuid", "id"]].rename(
                    columns={
                        "timestamp": f"{prefix}_timestamp",
                        "uuid": f"{prefix}_status_uuid",
                        "id": f"{prefix}_status_id"
                    }
                )
                return latest

            sent_flat = get_latest_status("sent", "sent")
            delivered_flat = get_latest_status("delivered", "delivered")
            read_flat = get_latest_status("read", "read")
            failed_flat = get_latest_status("failed", "failed")
            deleted_flat = get_latest_status("deleted", "deleted")

            status_wide = (
                counts_df.merge(sent_flat, on="message_uuid", how="left")
                .merge(delivered_flat, on="message_uuid", how="left")
                .merge(read_flat, on="message_uuid", how="left")
                .merge(failed_flat, on="message_uuid", how="left")
                .merge(deleted_flat, on="message_uuid", how="left")
            )

            for c in pivot_statuses:
                if c in status_wide.columns:
                    status_wide[c] = status_wide[c].fillna(0).astype("int64")

        # Prepare message data
        msg = self.messages_df.copy()
        for col in [
            "inserted_at",
            "updated_at",
            "external_timestamp",
            "last_status_timestamp",
        ]:
            if col in msg.columns:
                msg[col] = pd.to_datetime(msg[col], errors="coerce")

        rename_map = {
            "id": "message_id",
            "uuid": "message_uuid",
            "inserted_at": "message_inserted_at",
            "updated_at": "message_updated_at",
            "last_status": "msg_last_status_raw",
            "last_status_timestamp": "msg_last_status_timestamp_raw",
        }
        msg = msg.rename(columns=rename_map)

        desired_message_columns = [
            "message_id",
            "message_uuid",
            "message_type",
            "masked_addressees",
            "masked_author",
            "content",
            "author_type",
            "direction",
            "external_id",
            "external_timestamp",
            "masked_from_addr",
            "is_deleted",
            "rendered_content",
            "source_type",
            "message_inserted_at",
            "message_updated_at",
            "msg_last_status_raw",
            "msg_last_status_timestamp_raw",
        ]
        # Add missing columns to maintain schema
        for col in desired_message_columns:
            if col not in msg.columns:
                msg[col] = None
        msg_base = msg[desired_message_columns]

        # Join messages with status data
        if not status_wide.empty:
            unified = msg_base.merge(
                status_wide, left_on="message_uuid", right_on="message_uuid", how="left"
            )
        else:
            # No status data, add empty columns
            unified = msg_base.copy()
            for col in [
                "sent",
                "delivered",
                "read",
                "failed",
                "deleted",
                "sent_timestamp",
                "sent_inserted_at",
                "sent_updated_at",
                "sent_status_uuid",
                "sent_status_id",
                "sent_status_message_id",
                "sent_status_number_id",
                "delivered_timestamp",
                "delivered_inserted_at",
                "delivered_updated_at",
                "delivered_status_uuid",
                "delivered_status_id",
                "delivered_status_message_id",
                "delivered_status_number_id",
                "read_timestamp",
                "read_inserted_at",
                "read_updated_at",
                "read_status_uuid",
                "read_status_id",
                "read_status_message_id",
                "read_status_number_id",
                "failed_timestamp",
                "failed_inserted_at",
                "failed_updated_at",
                "failed_status_uuid",
                "failed_status_id",
                "failed_status_message_id",
                "failed_status_number_id",
                "deleted_timestamp",
                "deleted_inserted_at",
                "deleted_updated_at",
                "deleted_status_uuid",
                "deleted_status_id",
                "deleted_status_message_id",
                "deleted_status_number_id",
            ]:
                unified[col] = (
                    0
                    if col in ["sent", "delivered", "read", "failed", "deleted"]
                    else None
                )

        # Save status table for later use
        self.status_flat = status_wide.copy()

        logger.info(
            f"Created unified view with {len(unified)} records and {len(unified.columns)} columns"
        )
        return unified

    def detect_duplicates(self) -> List[DuplicateRecord]:
        """Find duplicate messages based on identical content"""
        logger.info("Detecting duplicates...")

        duplicates = []

        # Find messages with identical content
        if not self.messages_df.empty:
            content_groups = self.messages_df.groupby("content")

            for content, group in content_groups:
                if len(group) > 1 and content and str(content).strip():
                    # Create duplicate records
                    for _, row in group.iterrows():
                        try:
                            duplicate = DuplicateRecord.from_pandas_row(row)
                            duplicates.append(duplicate)
                        except Exception as e:
                            logger.warning(f"Failed to create duplicate record: {e}")
                            continue

        logger.info(f"Found {len(duplicates)} potential duplicate records")
        return duplicates

    def data_quality_checks(self) -> DataQualityReport:
        """Run data quality checks"""
        logger.info("Running data quality checks...")

        # Check data quality
        total_messages = len(self.messages_df)
        total_statuses = len(self.statuses_df)

        # Messages without required fields
        if not self.messages_df.empty and all(col in self.messages_df.columns for col in ["id", "uuid", "inserted_at"]):
            missing_required = self.messages_df[["id", "uuid", "inserted_at"]].isnull().any(axis=1).sum()
        else:
            missing_required = 0

        # Invalid status values
        valid_statuses = ["sent", "delivered", "read", "failed", "deleted"]
        if not self.statuses_df.empty and "status" in self.statuses_df.columns:
            invalid_statuses = len(self.statuses_df[~self.statuses_df["status"].isin(valid_statuses)])
        else:
            invalid_statuses = 0

        # Create basic report
        checks = [
            QualityCheckResult(
                check_name="total_messages",
                check_type="count",
                value=total_messages,
                description="Total messages processed",
                severity="info",
            ),
            QualityCheckResult(
                check_name="total_statuses",
                check_type="count",
                value=total_statuses,
                description="Total status records processed",
                severity="info",
            ),
            QualityCheckResult(
                check_name="missing_required_fields",
                check_type="count",
                value=missing_required,
                description="Records missing required fields",
                severity="error" if missing_required > 0 else "info",
            ),
            QualityCheckResult(
                check_name="invalid_statuses",
                check_type="count",
                value=invalid_statuses,
                description="Status records with invalid values",
                severity="error" if invalid_statuses > 0 else "info",
            ),
        ]

        report = DataQualityReport(
            total_messages=total_messages,
            total_statuses=total_statuses,
            messages_without_status=0,
            invalid_statuses=invalid_statuses,
            future_timestamps=0,
            missing_required_fields=missing_required,
            duplicates_found=0,
            checks=checks,
        )

        logger.info("Data quality checks completed")
        return report

    def transform_all(self) -> Dict[str, Any]:
        """Run all transformations using SQL-style approach"""
        logger.info("Running transformation pipeline...")

        # Clean data
        self.clean_data()

        # Create unified message view
        unified_messages = self.transform_unified_view()

        # Find duplicate messages
        duplicates = self.detect_duplicates()

        # Run quality checks
        quality_report = self.data_quality_checks()

        # Update duplicates count in quality report
        quality_report.duplicates_found = len(duplicates)

        results = {
            "unified_messages": unified_messages,
            "duplicates": duplicates,
            "quality_report": quality_report,
        }

        logger.info("Transformation pipeline completed successfully!")
        return results



