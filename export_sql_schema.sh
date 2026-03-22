#!/bin/bash
# Export CREATE TABLE statements from SQLAlchemy models to sql/create_table/

set -e

echo "Activating virtual environment..."
source venv/bin/activate

echo "Exporting database schema..."
python scripts/export_create_tables.py

echo ""
echo "Done! SQL files exported to sql/create_table/"
