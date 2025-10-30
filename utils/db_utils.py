import psycopg2
import os
from dotenv import load_dotenv
from utils.logger import logger

load_dotenv()


def init_sync_status_table(schema="hubspot"):
    """Crea la tabla de control de sincronización incremental."""
    conn = get_db_connection(schema=schema)
    cursor = conn.cursor()
    query = """
    CREATE TABLE IF NOT EXISTS sync_status (
        entity VARCHAR(50) PRIMARY KEY,
        last_sync TIMESTAMP
    );
    """
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("✅ Tabla 'sync_status' verificada o creada.")


def get_db_connection(schema: str = None):
    """Conecta a PostgreSQL y, si se pasa schema, lo define como search_path."""
    try:
        schema_option = f"-c search_path={schema}" if schema else ""
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT"),
            database=os.getenv("PG_DB"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            options=schema_option
        )
        logger.info(f"✅ Conexión exitosa (schema activo: {schema or 'public'})")
        return conn
    except Exception as e:
        logger.error(f"❌ Error al conectar a PostgreSQL: {e}")
        return None


def init_schema(schema="hubspot"):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"✅ Esquema '{schema}' verificado o creado.")
    init_sync_status_table(schema)


def init_contacts_table(schema="hubspot"):
    """Crea la tabla de contactos si no existe."""
    conn = get_db_connection(schema=schema)
    cursor = conn.cursor()
    query = """
    CREATE TABLE IF NOT EXISTS contacts (
        id SERIAL PRIMARY KEY,
        hs_object_id VARCHAR(50) UNIQUE,
        firstname VARCHAR(255),
        lastname VARCHAR(255),
        email VARCHAR(255),
        phone VARCHAR(50),
        createdate TIMESTAMP,
        lastmodifieddate TIMESTAMP
    );
    """
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("✅ Tabla 'contacts' verificada o creada.")


def init_deals_table(schema="hubspot"):
    """Crea la tabla de deals si no existe."""
    conn = get_db_connection(schema=schema)
    cursor = conn.cursor()
    query = """
    CREATE TABLE IF NOT EXISTS deals (
        id SERIAL PRIMARY KEY,
        hs_object_id VARCHAR(50) UNIQUE,
        dealname VARCHAR(255),
        dealstage VARCHAR(100),
        pipeline VARCHAR(100),
        amount NUMERIC(15,2),
        closedate TIMESTAMP,
        createdate TIMESTAMP,
        lastmodifieddate TIMESTAMP
    );
    """
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("✅ Tabla 'deals' verificada o creada.")


def init_leads_table(schema="hubspot"):
    """Crea la tabla de leads si no existe."""
    conn = get_db_connection(schema=schema)
    cursor = conn.cursor()
    query = """
    CREATE TABLE IF NOT EXISTS leads (
        id SERIAL PRIMARY KEY,
        hs_object_id VARCHAR(50) UNIQUE,
        firstname VARCHAR(255),
        lastname VARCHAR(255),
        email VARCHAR(255),
        phone VARCHAR(50),
        lifecyclestage VARCHAR(50),
        createdate TIMESTAMP,
        lastmodifieddate TIMESTAMP
    );
    """
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("✅ Tabla 'leads' verificada o creada.")


def init_engagements_table(schema="hubspot"):
    """Crea la tabla de engagements (emails) si no existe."""
    conn = get_db_connection(schema=schema)
    cursor = conn.cursor()
    query = """
    CREATE TABLE IF NOT EXISTS engagements (
        id SERIAL PRIMARY KEY,
        hs_object_id VARCHAR(50) UNIQUE,
        hs_email_direction VARCHAR(20),
        hs_timestamp TIMESTAMP,
        hs_from_email VARCHAR(255),
        hs_to_email VARCHAR(255),
        hs_subject TEXT
    );
    """
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("✅ Tabla 'engagements' verificada o creada.")

def init_sync_status_table(schema="hubspot"):
    """Crea la tabla de control de sincronización incremental."""
    conn = get_db_connection(schema=schema)
    cursor = conn.cursor()
    query = """
    CREATE TABLE IF NOT EXISTS sync_status (
        entity VARCHAR(50) PRIMARY KEY,
        last_sync TIMESTAMP
    );
    """
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("✅ Tabla 'sync_status' verificada o creada.")

