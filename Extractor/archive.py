import os
import logging
from string import Template
from datetime import datetime

import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from Extractor.database_connector import DatabaseConnector

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

# Config path
CONFIG_PATH = 'config.yaml'

def load_config(config_path):
    with open(config_path, 'r') as file:
        raw_config = file.read()
        substituted = Template(raw_config).substitute(os.environ)
        return yaml.safe_load(substituted)

def get_table_columns(engine, schema, table):
    query = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"schema": schema, "table": table})
        return [row[0] for row in result]

def archive_table(engine, source_schema, source_table, archive_schema, archive_table):
    source_columns = get_table_columns(engine, source_schema, source_table)
    archive_columns = get_table_columns(engine, archive_schema, archive_table)

    common_columns = list(set(source_columns) & set(archive_columns))
    if 'archived_at' in archive_columns:
        common_columns.append('archived_at')

    column_list = ', '.join(common_columns)
    select_list = ', '.join([
        'CURRENT_TIMESTAMP' if col == 'archived_at' else col for col in common_columns
    ])

    query = f"""
        INSERT INTO {archive_schema}.{archive_table} ({column_list})
        SELECT {select_list}
        FROM {source_schema}.{source_table}
    """

    with engine.connect() as conn:
        result = conn.execute(text(query))
        logging.info(f"Archived {result.rowcount} rows from {source_table} → {archive_table}")

def main():
    logging.info("Starting archive process...")
    config = load_config(CONFIG_PATH)
    print(config)

    db_config = config['database']
    schemas = config['schemas']
    tables = config['tables']

    connector = DatabaseConnector(config)

    engine = connector.get_engine()

    all_tables = tables.get('s3', []) + tables.get('api', []) + tables.get('json', [])

    for table in all_tables:
        archive_table(
            engine,
            source_schema=schemas['landing'],
            source_table=table,
            archive_schema=schemas['archive'],
            archive_table=f"archive_{table}"
        )

    engine.dispose()
    logging.info("Archiving complete.")

if __name__ == '__main__':
    main()
