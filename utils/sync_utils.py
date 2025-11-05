import io
from utils.db_utils import get_db_connection
from utils.logger import logger
from datetime import datetime
import time

def parse_date(date_str):
    if not date_str:
        return ""
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except Exception:
        return ""

def save_contacts_to_db(contacts, schema="hubspot"):
    start = time.time()
    total_contacts = len(contacts)
    print(f"📥 Iniciando inserción masiva de {total_contacts} contactos usando COPY...")

    if not contacts:
        print("⚠️ No hay contactos para insertar")
        return

    conn = get_db_connection(schema=schema)
    if not conn:
        print("❌ No se pudo conectar a la base de datos")
        return

    cursor = conn.cursor()

    buffer = io.StringIO()
    for contact in contacts:
        props = contact.properties
        hs_object_id = props.get("hs_object_id")
        if not hs_object_id:
            continue

        row = [
            str(hs_object_id),
            props.get("firstname", "") or "",
            props.get("lastname", "") or "",
            props.get("email", "") or "",
            props.get("phone", "") or "",
            parse_date(props.get("createdate")),
            parse_date(props.get("lastmodifieddate"))
        ]

        # Convertimos a texto separados por tabulaciones y escapamos caracteres especiales
        buffer.write('\t'.join(str(v).replace('\n', ' ').replace('\r', ' ') for v in row) + '\n')

    buffer.seek(0)

    try:
        cursor.copy_expert(f"""
            COPY contacts (hs_object_id, firstname, lastname, email, phone, createdate, lastmodifieddate)
            FROM STDIN WITH (FORMAT text)
        """, buffer)

        conn.commit()
        print(f"✅ {total_contacts} contactos insertados exitosamente usando COPY.")
        logger.info(f"⚡ {total_contacts} contactos insertados en {schema}.contacts con COPY ✅")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error al usar COPY: {e}")
        logger.error(f"❌ Error al usar COPY: {e}")

    finally:
        cursor.close()
        conn.close()

    print(f"⏱️ Tiempo total: {round(time.time() - start, 2)} segundos")