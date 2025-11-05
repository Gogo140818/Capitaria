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

def get_all_contacts(max_contacts=10000):
    """Obtiene contactos hasta el límite máximo de HubSpot"""
    client = get_hubspot_client()
    PROPERTIES = ["firstname", "lastname", "email", "phone", "createdate", "lastmodifieddate", "hs_object_id"]
    
    all_contacts = []
    after = None
    limit = 100
    total_processed = 0
    
    try:
        logger.info(f"📡 Obteniendo hasta {max_contacts} contactos de HubSpot...")
        
        while total_processed < max_contacts:
            search_request = PublicObjectSearchRequest(
                limit=limit,
                properties=PROPERTIES
            )
            
            if after:
                search_request.after = after
            
            search_result = client.crm.contacts.search_api.do_search(
                public_object_search_request=search_request
            )
            
            batch_contacts = search_result.results
            all_contacts.extend(batch_contacts)
            total_processed += len(batch_contacts)
            
            print(f"📦 Lote {len(all_contacts)//100}: {len(batch_contacts)} contactos (Total: {total_processed})")
            
            # Verificar si hay más páginas y no exceder el límite
            if (hasattr(search_result, 'paging') and search_result.paging and
                hasattr(search_result.paging, 'next') and search_result.paging.next and
                total_processed < max_contacts):
                
                after = search_result.paging.next.after
            else:
                print("✅ Llegamos al final de los contactos disponibles")
                break
                
            # Pequeña pausa para no sobrecargar la API
            time.sleep(0.1)
                
        print(f"✅ {len(all_contacts)} contactos obtenidos en total")
        return all_contacts
        
    except Exception as e:
        print(f"❌ Error obteniendo contactos: {e}")
        # Devolver los contactos que ya obtuvimos antes del error
        if all_contacts:
            print(f"⚠️ Pero se obtuvieron {len(all_contacts)} contactos antes del error")
            return all_contacts
        return []

def get_contacts_batch(limit=100):
    """Función original para compatibilidad"""
    return get_all_contacts(limit)