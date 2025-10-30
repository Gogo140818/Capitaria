from utils.db_utils import get_db_connection
from datetime import datetime, timezone
from utils.logger import logger

def get_last_sync_time(entity, schema="hubspot"):
    """Obtiene la última fecha de sincronización de una entidad."""
    conn = get_db_connection(schema=schema)
    cursor = conn.cursor()
    cursor.execute("SELECT last_sync FROM sync_status WHERE entity = %s;", (entity,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    if result and result[0]:
        logger.info(f"🕒 Última sync de {entity}: {result[0]}")
        return result[0]
    return None


def update_last_sync_time(entity, schema="hubspot"):
    """Actualiza o inserta la fecha actual como última sincronización."""
    conn = get_db_connection(schema=schema)
    cursor = conn.cursor()
    now = datetime.now(timezone.utc)
    cursor.execute("""
        INSERT INTO sync_status (entity, last_sync)
        VALUES (%s, %s)
        ON CONFLICT (entity)
        DO UPDATE SET last_sync = EXCLUDED.last_sync;
    """, (entity, now))
    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"🕒 Timestamp de sync actualizado para {entity}: {now.isoformat()}")
