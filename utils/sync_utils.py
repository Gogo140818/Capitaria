import time
from datetime import datetime
from psycopg2.extras import execute_values
from utils.db_utils import get_db_connection
from utils.logger import logger


def parse_date(date_str):
    """Convierte fechas ISO 8601 (de HubSpot) a datetime."""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except Exception:
        return None


def bulk_insert(cursor, query, data, batch_size=10000):
    """Inserta datos en lotes grandes usando execute_values."""
    total = len(data)
    for i in range(0, total, batch_size):
        batch = data[i:i + batch_size]
        execute_values(cursor, query, batch, page_size=batch_size)
    return total


# ---------- CONTACTOS ----------
def save_contacts_to_db(contacts, schema="hubspot"):
    start = time.time()
    conn = get_db_connection(schema=schema)
    cursor = conn.cursor()

    query = """
        INSERT INTO contacts (hs_object_id, firstname, lastname, email, phone, createdate, lastmodifieddate)
        VALUES %s
        ON CONFLICT (hs_object_id) DO UPDATE
        SET firstname = EXCLUDED.firstname,
            lastname = EXCLUDED.lastname,
            email = EXCLUDED.email,
            phone = EXCLUDED.phone,
            lastmodifieddate = EXCLUDED.lastmodifieddate;
    """

    data = [
        (
            c.properties.get("hs_object_id"),
            c.properties.get("firstname"),
            c.properties.get("lastname"),
            c.properties.get("email"),
            c.properties.get("phone"),
            parse_date(c.properties.get("createdate")),
            parse_date(c.properties.get("lastmodifieddate")),
        )
        for c in contacts
        if c.properties.get("hs_object_id")
    ]

    try:
        count = bulk_insert(cursor, query, data)
        conn.commit()
        logger.info(f"⚡ {count} contactos insertados/actualizados en {schema}.contacts ✅")
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Error en bulk insert de contactos: {e}")
    finally:
        cursor.close()
        conn.close()

    logger.info(f"⏱️ Tiempo total contactos: {round(time.time() - start, 2)}s")


# ---------- DEALS ----------
def save_deals_to_db(deals, schema="hubspot"):
    start = time.time()
    conn = get_db_connection(schema=schema)
    cursor = conn.cursor()

    query = """
        INSERT INTO deals (hs_object_id, dealname, dealstage, pipeline, amount, closedate, createdate, lastmodifieddate)
        VALUES %s
        ON CONFLICT (hs_object_id) DO UPDATE
        SET dealname = EXCLUDED.dealname,
            dealstage = EXCLUDED.dealstage,
            pipeline = EXCLUDED.pipeline,
            amount = EXCLUDED.amount,
            closedate = EXCLUDED.closedate,
            lastmodifieddate = EXCLUDED.lastmodifieddate;
    """

    data = [
        (
            d.properties.get("hs_object_id"),
            d.properties.get("dealname"),
            d.properties.get("dealstage"),
            d.properties.get("pipeline"),
            float(d.properties.get("amount")) if d.properties.get("amount") else None,
            parse_date(d.properties.get("closedate")),
            parse_date(d.properties.get("createdate")),
            parse_date(d.properties.get("lastmodifieddate")),
        )
        for d in deals
        if d.properties.get("hs_object_id")
    ]

    try:
        count = bulk_insert(cursor, query, data)
        conn.commit()
        logger.info(f"⚡ {count} deals insertados/actualizados en {schema}.deals ✅")
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Error en bulk insert de deals: {e}")
    finally:
        cursor.close()
        conn.close()

    logger.info(f"⏱️ Tiempo total deals: {round(time.time() - start, 2)}s")


# ---------- LEADS ----------
def save_leads_to_db(leads, schema="hubspot"):
    start = time.time()
    conn = get_db_connection(schema=schema)
    cursor = conn.cursor()

    query = """
        INSERT INTO leads (hs_object_id, firstname, lastname, email, phone, lifecyclestage, createdate, lastmodifieddate)
        VALUES %s
        ON CONFLICT (hs_object_id) DO UPDATE
        SET firstname = EXCLUDED.firstname,
            lastname = EXCLUDED.lastname,
            email = EXCLUDED.email,
            phone = EXCLUDED.phone,
            lifecyclestage = EXCLUDED.lifecyclestage,
            lastmodifieddate = EXCLUDED.lastmodifieddate;
    """

    data = [
        (
            l.properties.get("hs_object_id"),
            l.properties.get("firstname"),
            l.properties.get("lastname"),
            l.properties.get("email"),
            l.properties.get("phone"),
            l.properties.get("lifecyclestage"),
            parse_date(l.properties.get("createdate")),
            parse_date(l.properties.get("lastmodifieddate")),
        )
        for l in leads
        if l.properties.get("hs_object_id")
    ]

    try:
        count = bulk_insert(cursor, query, data)
        conn.commit()
        logger.info(f"⚡ {count} leads insertados/actualizados en {schema}.leads ✅")
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Error en bulk insert de leads: {e}")
    finally:
        cursor.close()
        conn.close()

    logger.info(f"⏱️ Tiempo total leads: {round(time.time() - start, 2)}s")


# ---------- ENGAGEMENTS ----------
def save_engagements_to_db(engagements, schema="hubspot"):
    start = time.time()
    conn = get_db_connection(schema=schema)
    cursor = conn.cursor()

    query = """
        INSERT INTO engagements (hs_object_id, hs_email_direction, hs_timestamp, hs_from_email, hs_to_email, hs_subject)
        VALUES %s
        ON CONFLICT (hs_object_id) DO UPDATE
        SET hs_email_direction = EXCLUDED.hs_email_direction,
            hs_timestamp = EXCLUDED.hs_timestamp,
            hs_from_email = EXCLUDED.hs_from_email,
            hs_to_email = EXCLUDED.hs_to_email,
            hs_subject = EXCLUDED.hs_subject;
    """

    data = [
        (
            e.properties.get("hs_object_id"),
            e.properties.get("hs_email_direction"),
            parse_date(e.properties.get("hs_timestamp")),
            e.properties.get("hs_from_email"),
            e.properties.get("hs_to_email"),
            e.properties.get("hs_subject"),
        )
        for e in engagements
        if e.properties.get("hs_object_id")
    ]

    try:
        count = bulk_insert(cursor, query, data)
        conn.commit()
        logger.info(f"⚡ {count} engagements insertados/actualizados en {schema}.engagements ✅")
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Error en bulk insert de engagements: {e}")
    finally:
        cursor.close()
        conn.close()

    logger.info(f"⏱️ Tiempo total engagements: {round(time.time() - start, 2)}s")
