# app/config.py
# Version 1.0 — 2025-01-01
#
# Database configuration for The Queue
# Uses Windows Authentication to connect to Epicor SQL Server

import os
from sqlalchemy import create_engine

# Connection String for EPIC10LIVE using Windows Authentication
CONN_STR = (
    "mssql+pyodbc://@SQL1.CORP.JD2.COM/EPIC10LIVE"
    "?driver=ODBC+Driver+18+for+SQL+Server"
    "&trusted_connection=yes&TrustServerCertificate=yes"
)

# PDF path translation (Epicor stores UNC paths, we need local paths)
PDF_UNC_PREFIX = os.getenv('PDF_UNC_PREFIX', r'\\JAIMEE-EF\EPICOR\Part Attachments')
PDF_LOCAL_PREFIX = os.getenv('PDF_LOCAL_PREFIX', r'C:\EPICOR\Part Attachments')

_engine = None

def get_engine():
    """Get or create the SQLAlchemy engine."""
    global _engine
    if _engine is None:
        _engine = create_engine(CONN_STR, fast_executemany=True)
    return _engine


def translate_pdf_path(unc_path):
    """Convert UNC path from Epicor to local path on server."""
    if not unc_path:
        return None
    # Replace UNC prefix with local prefix
    if unc_path.startswith(PDF_UNC_PREFIX):
        return unc_path.replace(PDF_UNC_PREFIX, PDF_LOCAL_PREFIX, 1)
    # If it doesn't match UNC prefix, return as-is (might already be local)
    return unc_path
