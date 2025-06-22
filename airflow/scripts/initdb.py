import logging
from datetime import timedelta
from scripts import logger_config, property_manager # noqa: F401
from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine, text
import os

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

logger = logging.getLogger(__name__)

def init_db(partition_date=None):
    database_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    default_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/postgres"
    create_database_if_not_exists(database_url, default_url)
    create_tables_if_not_exist(database_url)
    if partition_date:
        create_partitions(database_url, partition_date)

# Check/Create DB
def create_database_if_not_exists(database_url, default_url):
    try:
        engine = create_engine(database_url)
        engine.connect()
    except OperationalError as e:
        logger.warning(f"Database does not exist. Attempting to create '{DB_NAME}'")
        engine = create_engine(default_url, isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE {DB_NAME}"))
            logger.info(f"Created database: {DB_NAME}")

# Check/Create Tables
def create_tables_if_not_exist(database_url):
    engine = create_engine(database_url, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        # Check PostGIS extension
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))

        # Check if table smap exists
        result = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = 'smap'
            );
        """))
        if not result.scalar():
            # Create smap table
            logger.warning(f"Table does not exist. Attempting to create table smap.")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS smap(
                    location geometry(Point, 4326) NOT NULL,
                    date DATE NOT NULL,
                    soil_moisture FLOAT8
                )
                PARTITION BY RANGE (date);
            """))

            logger.info("Created table: smap")

        # Check if table cygnss exists
        result = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = 'cygnss'
            );
        """))
        if not result.scalar():
            # Create cygnss table
            logger.warning(f"Table does not exist. Attempting to create table cygnss.")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS cygnss(
                    location geometry(Point, 4326) NOT NULL,
                    date DATE NOT NULL,
                    reflectivity FLOAT8,
                    incident_angle FLOAT8,
                    antenna_gain FLOAT8,
                    ddm_peak_delay SMALLINT
                )
                PARTITION BY RANGE (date);
            """))

            conn.execute(text("""CREATE INDEX idx_spatial_geom_cygnss
                ON cygnss
                USING GIST (location)
            """))
            logger.info("Created table: cygnss")


# create partitions for database
def create_partitions(database_url, partition_date):
    engine = create_engine(database_url, isolation_level="AUTOCOMMIT")
    date_str = partition_date.strftime('%Y%m%d')
    smap_partition_table = f"smap_{date_str}"
    smap_index = f"idx_{smap_partition_table}_geom"
    cygnss_partition_table = f"cygnss_{date_str}"
    cygnss_index = f"idx_{cygnss_partition_table}_geom"

    start_date = partition_date.strftime("'%Y-%m-%d'")
    end_date = (partition_date + timedelta(days=1)).strftime("'%Y-%m-%d'")

    with engine.connect() as conn:
        # create partition for smap
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {smap_partition_table}
            PARTITION OF smap
            FOR VALUES FROM ({start_date}) TO ({end_date});
        """))

        logger.info(f"Created partition table: {smap_partition_table}")

        # create index for smap partition
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS {smap_index}
            ON {smap_partition_table}
            USING GIST (location);
        """))

        logger.info(f"Created index: {smap_index}")

        # create partition for cygnss
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {cygnss_partition_table}
            PARTITION OF cygnss
            FOR VALUES FROM ({start_date}) TO ({end_date});
        """))

        logger.info(f"Created partition table: {cygnss_partition_table}")

        # create index for cygnss partition
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS {cygnss_index}
            ON {cygnss_partition_table}
            USING GIST (location);
        """))

        logger.info(f"Created index: {cygnss_index}")


if __name__ == "__main__":
    init_db()