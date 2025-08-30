#!/usr/bin/env python3

import pandas as pd
import logging
from typing import Tuple

logger = logging.getLogger(__name__)


class GSheetsExtractor:
    """Simple Google Sheets extractor using pandas"""

    def __init__(self, data_dir: str = None):
        # Google Sheets export URLs for different tabs
        base_url = "https://docs.google.com/spreadsheets/d/1XC0YaSQ4WjLwhzCB96RxF23-NjFga1Fisr-9lX_7hmk/export?format=csv"
        self.messages_url = f"{base_url}&gid=1033608769"  # Messages tab
        self.statuses_url = f"{base_url}&gid=966707183"  # Statuses tab

    def _read_sheet_data(self, url: str, sheet_name: str) -> pd.DataFrame:
        """Read data from specific Google Sheet tab"""
        try:
            logger.info(f"Reading data from {sheet_name} tab")
            df = pd.read_csv(url)
            logger.info(f"Extracted {len(df)} rows from {sheet_name}")
            return df
        except Exception as e:
            logger.warning(f"Failed to read {sheet_name}: {e}")
            return pd.DataFrame()

    def extract_messages(self) -> pd.DataFrame:
        """Extract messages data from Messages tab"""
        df = self._read_sheet_data(self.messages_url, "Messages")
        logger.info(f"Extracting messages: {len(df)} rows")
        return df

    def extract_statuses(self) -> pd.DataFrame:
        """Extract statuses data from Statuses tab"""
        df = self._read_sheet_data(self.statuses_url, "Statuses")
        logger.info(f"Extracting statuses: {len(df)} rows")
        return df

    def extract_all(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Extract both datasets"""
        messages_df = self.extract_messages()
        statuses_df = self.extract_statuses()
        return messages_df, statuses_df