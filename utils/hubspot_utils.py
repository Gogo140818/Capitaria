import os
from dotenv import load_dotenv
from hubspot import HubSpot
from hubspot.crm.contacts import PublicObjectSearchRequest
from utils.logger import logger
import time

load_dotenv()
ACCESS_TOKEN = os.getenv("HUBSPOT_TOKEN")

def get_hubspot_client():
    return HubSpot(access_token=ACCESS_TOKEN)

def get_all_contacts():
    """Obtiene todos los contactos de HubSpot usando paginación"""
    client = get_hubspot_client()
    PROPERTIES = ["firstname", "lastname", "email", "phone", "createdate", "lastmodifieddate", "hs_object_id"]

    all_contacts = []
    after = None
    limit = 100
    lote = 1

    try:
        logger.info("📡 Obteniendo todos los contactos de HubSpot...")

        while True:
            response = client.crm.contacts.basic_api.get_page(
                limit=limit,
                after=after,
                properties=PROPERTIES
            )

            results = response.results
            all_contacts.extend(results)

            print(f"📦 Lote {lote}: {len(results)} contactos (Total: {len(all_contacts)})")
            lote += 1

            if hasattr(response, "paging") and response.paging and hasattr(response.paging, "next") and response.paging.next:
                after = response.paging.next.after
            else:
                print("✅ Se obtuvieron todos los contactos disponibles")
                break

            time.sleep(0.2)  # ligera pausa para evitar rate limits

        print(f"✅ Total final: {len(all_contacts)} contactos obtenidos")
        return all_contacts

    except Exception as e:
        print(f"❌ Error obteniendo contactos: {e}")
        if all_contacts:
            print(f"⚠️ Pero se obtuvieron {len(all_contacts)} contactos antes del error")
            return all_contacts
        return []



def get_contacts_batch(limit=100):
    """Función original para compatibilidad"""
    return get_all_contacts(limit)