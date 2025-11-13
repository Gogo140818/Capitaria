import psycopg2
import os
import io
import time
from dotenv import load_dotenv
from utils.logger import logger

load_dotenv()


# ============================================================
# CONEXIÓN
# ============================================================

def get_db_connection(schema: str = None):
    """Conecta a PostgreSQL y, si se pasa schema, lo define como search_path."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT"),
            database=os.getenv("PG_DB"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD")
        )

        if schema:
            cursor = conn.cursor()
            cursor.execute(f"SET search_path TO {schema}")
            conn.commit()
            cursor.close()

        logger.info(f"✅ Conexión exitosa a {os.getenv('PG_DB')} (schema: {schema or 'public'})")
        return conn
    except Exception as e:
        logger.error(f"❌ Error al conectar a PostgreSQL: {e}")
        return None


# ============================================================
# SCHEMAS Y TABLAS BASE
# ============================================================

def init_schema(schema="hubspot"):
    """Crea el esquema si no existe."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"✅ Esquema '{schema}' verificado o creado.")


def init_contacts_table(schema="hubspot"):
    """Crea la tabla de contactos si no existe."""
    conn = get_db_connection(schema=schema)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS contacts (
            id SERIAL PRIMARY KEY,
            hs_object_id VARCHAR(50) UNIQUE,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            email VARCHAR(255),
            phone VARCHAR(50),
            createdate TIMESTAMP,
            lastmodifieddate TIMESTAMP,
            pais VARCHAR(100),
            comuna VARCHAR(100),
            created_in_salesforce VARCHAR(10),
            send_to_salesforce VARCHAR(10),
            respuesta_primera_interaccion VARCHAR(255),
            etapa_vambe VARCHAR(100),
            contactado_vambe VARCHAR(100),
            hs_object_source_detail_1 VARCHAR(255),
            scoring_clon VARCHAR(100)
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    logger.info("✅ Tabla 'contacts' verificada o creada.")


# ============================================================
# TABLA TEMP_CONTACTS Y UPSERT
# ============================================================

def init_temp_contacts_table(schema="hubspot"):
    """Crea la tabla física hubspot.temp_contacts si no existe."""
    conn = get_db_connection()  # sin schema para evitar SET search_path
    cur = conn.cursor()

    # Crear schema y tabla explícitamente
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.temp_contacts (
            hs_object_id VARCHAR(50),
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            email VARCHAR(255),
            phone VARCHAR(50),
            createdate TIMESTAMP,
            lastmodifieddate TIMESTAMP,
            pais VARCHAR(100),
            comuna VARCHAR(100),
            created_in_salesforce VARCHAR(10),
            send_to_salesforce VARCHAR(10),
            respuesta_primera_interaccion VARCHAR(255),
            etapa_vambe VARCHAR(100),
            contactado_vambe VARCHAR(100),
            hs_object_source_detail_1 VARCHAR(255),
            scoring_clon VARCHAR(100)
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"🧩 Tabla '{schema}.temp_contacts' verificada o creada correctamente.")


def save_contacts_to_db(contacts, schema="hubspot"):
    """Guarda los contactos en base de datos usando COPY + UPSERT forzado al schema correcto."""
    start_time = time.time()
    conn = get_db_connection()  # sin schema, para no activar search_path
    cur = conn.cursor()

    # Asegurar tabla temporal
    init_temp_contacts_table(schema)

    try:
        # LIMPIAR TABLA TEMPORAL
        print(f"🧹 Limpiando tabla {schema}.temp_contacts...")
        cur.execute(f"TRUNCATE {schema}.temp_contacts;")
        conn.commit()

        # PREPARAR BUFFER PARA COPY
        buffer = io.StringIO()
        for c in contacts:
            p = c.properties
            buffer.write(
                f"{p.get('hs_object_id') or ''}\t"
                f"{p.get('firstname') or ''}\t"
                f"{p.get('lastname') or ''}\t"
                f"{p.get('email') or ''}\t"
                f"{p.get('phone') or ''}\t"
                f"{p.get('createdate') or ''}\t"
                f"{p.get('lastmodifieddate') or ''}\t"
                f"{p.get('pais') or ''}\t"
                f"{p.get('comuna') or ''}\t"
                f"{p.get('created_in_salesforce') or ''}\t"
                f"{p.get('send_to_salesforce') or ''}\t"
                f"{p.get('respuesta_primera_interaccion') or ''}\t"
                f"{p.get('etapa_vambe') or ''}\t"
                f"{p.get('contactado_vambe') or ''}\t"
                f"{p.get('hs_object_source_detail_1') or ''}\t"
                f"{p.get('scoring_clon') or ''}\n"
            )
        buffer.seek(0)

        # COPY → temp_contacts
        print(f"📥 Iniciando inserción masiva de {len(contacts)} contactos usando COPY...")
        copy_sql = f"""
            COPY {schema}.temp_contacts
            (
                hs_object_id,
                firstname,
                lastname,
                email,
                phone,
                createdate,
                lastmodifieddate,
                pais,
                comuna,
                created_in_salesforce,
                send_to_salesforce,
                respuesta_primera_interaccion,
                etapa_vambe,
                contactado_vambe,
                hs_object_source_detail_1,
                scoring_clon
            )
            FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '');
        """
        cur.copy_expert(copy_sql, buffer)
        conn.commit()
        print(f"✅ Inserción en {schema}.temp_contacts completada")

        # UPSERT → contacts
        print(f"🔁 Realizando UPSERT desde {schema}.temp_contacts hacia {schema}.contacts...")
        upsert_sql = f"""
            INSERT INTO {schema}.contacts AS c
            (
                hs_object_id,
                firstname,
                lastname,
                email,
                phone,
                createdate,
                lastmodifieddate,
                pais,
                comuna,
                created_in_salesforce,
                send_to_salesforce,
                respuesta_primera_interaccion,
                etapa_vambe,
                contactado_vambe,
                hs_object_source_detail_1,
                scoring_clon
            )
            SELECT
                hs_object_id,
                firstname,
                lastname,
                email,
                phone,
                createdate,
                lastmodifieddate,
                pais,
                comuna,
                created_in_salesforce,
                send_to_salesforce,
                respuesta_primera_interaccion,
                etapa_vambe,
                contactado_vambe,
                hs_object_source_detail_1,
                scoring_clon
            FROM {schema}.temp_contacts
            ON CONFLICT (hs_object_id)
            DO UPDATE SET
                firstname                  = EXCLUDED.firstname,
                lastname                   = EXCLUDED.lastname,
                email                      = EXCLUDED.email,
                phone                      = EXCLUDED.phone,
                lastmodifieddate           = EXCLUDED.lastmodifieddate,
                pais                       = EXCLUDED.pais,
                comuna                     = EXCLUDED.comuna,
                created_in_salesforce      = EXCLUDED.created_in_salesforce,
                send_to_salesforce         = EXCLUDED.send_to_salesforce,
                respuesta_primera_interaccion = EXCLUDED.respuesta_primera_interaccion,
                etapa_vambe                = EXCLUDED.etapa_vambe,
                contactado_vambe           = EXCLUDED.contactado_vambe,
                hs_object_source_detail_1  = EXCLUDED.hs_object_source_detail_1,
                scoring_clon               = EXCLUDED.scoring_clon;
        """
        cur.execute(upsert_sql)
        conn.commit()
        print(f"✅ UPSERT completado correctamente en {time.time() - start_time:.2f} segundos")

        # LIMPIEZA FINAL → eliminar contactos borrados en HubSpot
        print("🧹 Eliminando contactos que ya no existen en HubSpot...")
        delete_sql = f"""
            DELETE FROM {schema}.contacts
            WHERE hs_object_id NOT IN (
                SELECT hs_object_id FROM {schema}.temp_contacts
            );
        """
        cur.execute(delete_sql)
        conn.commit()
        print("✅ Limpieza completada (contactos antiguos eliminados)")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error al guardar contactos: {e}")

    finally:
        cur.close()
        conn.close()
