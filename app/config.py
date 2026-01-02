# app/config.py
# Version 1.0 — 2025-01-01
#
# Database configuration for The Queue
# Uses Windows Authentication to connect to Epicor SQL Server

from sqlalchemy import create_engine

# Connection String for EPIC10LIVE using Windows Authentication
CONN_STR = (
    "mssql+pyodbc://@SQL1.CORP.JD2.COM/EPIC10LIVE"
    "?driver=ODBC+Driver+18+for+SQL+Server"
    "&trusted_connection=yes&TrustServerCertificate=yes"
)

_engine = None

def get_engine():
    """Get or create the SQLAlchemy engine."""
    global _engine
    if _engine is None:
        _engine = create_engine(CONN_STR, fast_executemany=True)
    return _engine
