# run.py
# Version 1.0 — Production server for The Queue
# Uses Waitress, binds to 0.0.0.0

import sys

try:
    from app import create_app
except ImportError as e:
    print(f"Error importing app: {e}")
    sys.exit(1)

app = create_app()

if __name__ == '__main__':
    from waitress import serve
    HOST = '0.0.0.0'
    PORT = 5002
    print(f"[The Queue] Starting on http://{HOST}:{PORT}")
    serve(app, host=HOST, port=PORT, threads=4)
