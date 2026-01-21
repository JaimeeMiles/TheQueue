# app/config.py
# Version 1.3 — 2026-01-20
#
# Database configuration for The Queue
# Supports both Windows Auth and SQL Server Auth via DB_AUTH setting

import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine

# Read from environment
DB_SERVER = os.getenv('DB_SERVER', 'SQL1.CORP.JD2.COM')
DB_NAME = os.getenv('DB_NAME', 'ERP10LIVE')
ODBC_DRIVER = os.getenv('ODBC_DRIVER', 'ODBC Driver 17 for SQL Server')

# Database authentication mode: 'windows' or 'sql'
# If not specified, defaults to 'windows'
DB_AUTH = os.getenv('DB_AUTH', 'windows').lower()

# Database credentials (only used if DB_AUTH=sql)
DB_USERNAME = os.getenv('DB_USERNAME', '')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

# Build connection string dynamically
if DB_AUTH == 'sql' and DB_USERNAME and DB_PASSWORD:
    # SQL Server Authentication - URL encode password to handle special characters
    CONN_STR = (
        f"mssql+pyodbc://{quote_plus(DB_USERNAME)}:{quote_plus(DB_PASSWORD)}@{DB_SERVER}/{DB_NAME}"
        f"?driver={ODBC_DRIVER.replace(' ', '+')}"
        "&TrustServerCertificate=yes"
    )
else:
    # Windows Authentication (default)
    CONN_STR = (
        f"mssql+pyodbc://@{DB_SERVER}/{DB_NAME}"
        f"?driver={ODBC_DRIVER.replace(' ', '+')}"
        "&trusted_connection=yes&TrustServerCertificate=yes"
    )

# PDF path translation (Epicor stores UNC paths, we need local paths)
PDF_UNC_PREFIX = os.getenv('PDF_UNC_PREFIX', r'\\JAIMEE-EF\EPICOR\Part Attachments')
PDF_LOCAL_PREFIX = os.getenv('PDF_LOCAL_PREFIX', r'C:\EPICOR\Part Attachments')

# Epicor REST API
EPICOR_API_URL = os.getenv('EPICOR_API_URL', '')
EPICOR_API_KEY = os.getenv('EPICOR_API_KEY', '')
EPICOR_USERNAME = os.getenv('EPICOR_USERNAME', '')
EPICOR_PASSWORD = os.getenv('EPICOR_PASSWORD', '')

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
