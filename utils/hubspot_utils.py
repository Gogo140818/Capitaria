import os
from dotenv import load_dotenv
from hubspot import HubSpot
from hubspot.crm.contacts import (
    ApiException as ContactsApiException,
    PublicObjectSearchRequest,
    BatchReadInputSimplePublicObjectId
)
from hubspot.crm.deals import (
    ApiException as DealsApiException,
    PublicObjectSearchRequest as DealSearchRequest,
    BatchReadInputSimplePublicObjectId as DealBatchInput
)
from hubspot.crm.objects import (
    ApiException as ObjectsApiException,
    PublicObjectSearchRequest as ObjectSearchRequest,
    BatchReadInputSimplePublicObjectId as ObjectBatchInput
)
from utils.logger import logger

load_dotenv()
ACCESS_TOKEN = os.getenv("HUBSPOT_TOKEN")


def get_hubspot_client():
    return HubSpot(access_token=ACCESS_TOKEN)


# -------------------- CONTACTS --------------------
def get_contacts_batch(limit=10000):
    client = get_hubspot_client()
    PROPS = ["firstname", "lastname", "email", "phone", "createdate", "lastmodifieddate", "hs_object_id"]

    try:
        logger.info("📡 Obteniendo lista de IDs de contactos...")
        search_request = PublicObjectSearchRequest(limit=limit, properties=["hs_object_id"])
        search = client.crm.contacts.search_api.do_search(public_object_search_request=search_request)
        contact_ids = [r.id for r in search.results]

        if not contact_ids:
            logger.info("⚠️ No se encontraron contactos.")
            return []

        batch_input = BatchReadInputSimplePublicObjectId(inputs=[{"id": cid} for cid in contact_ids])
        response = client.crm.contacts.batch_api.read(batch_read_input_simple_public_object_id=batch_input, properties=PROPS)

        results = response.results
        logger.info(f"⚡ {len(results)} contactos obtenidos.")
        if results:
            logger.debug(f"📋 Ejemplo: {results[0].properties}")
        return results

    except ContactsApiException as e:
        logger.error(f"❌ Error al obtener contactos (Batch Read): {e}")
        return []


# -------------------- DEALS --------------------
def get_deals_batch(limit=10000):
    client = get_hubspot_client()
    PROPS = ["dealname", "dealstage", "pipeline", "amount", "closedate", "createdate", "lastmodifieddate", "hs_object_id"]

    try:
        logger.info("📡 Obteniendo lista de IDs de deals...")
        search_request = DealSearchRequest(limit=limit, properties=["hs_object_id"])
        search = client.crm.deals.search_api.do_search(public_object_search_request=search_request)
        deal_ids = [r.id for r in search.results]

        if not deal_ids:
            logger.info("⚠️ No se encontraron deals.")
            return []

        batch_input = DealBatchInput(inputs=[{"id": did} for did in deal_ids])
        response = client.crm.deals.batch_api.read(batch_read_input_simple_public_object_id=batch_input, properties=PROPS)

        results = response.results
        logger.info(f"⚡ {len(results)} deals obtenidos.")
        if results:
            logger.debug(f"📋 Ejemplo: {results[0].properties}")
        return results

    except DealsApiException as e:
        logger.error(f"❌ Error al obtener deals (Batch Read): {e}")
        return []


# -------------------- LEADS --------------------
def get_leads_batch(limit=10000):
    client = get_hubspot_client()
    PROPS = ["firstname", "lastname", "email", "phone", "lifecyclestage", "createdate", "lastmodifieddate", "hs_object_id"]

    try:
        logger.info("📡 Obteniendo lista de IDs de contactos para leads...")
        search_request = PublicObjectSearchRequest(limit=limit, properties=["hs_object_id", "lifecyclestage"])
        search = client.crm.contacts.search_api.do_search(public_object_search_request=search_request)

        lead_ids = [r.id for r in search.results if r.properties.get("lifecyclestage") == "lead"]

        if not lead_ids:
            logger.info("⚠️ No se encontraron leads.")
            return []

        batch_input = BatchReadInputSimplePublicObjectId(inputs=[{"id": lid} for lid in lead_ids])
        response = client.crm.contacts.batch_api.read(batch_read_input_simple_public_object_id=batch_input, properties=PROPS)

        results = response.results
        logger.info(f"⚡ {len(results)} leads obtenidos.")
        if results:
            logger.debug(f"📋 Ejemplo: {results[0].properties}")
        return results

    except ContactsApiException as e:
        logger.error(f"❌ Error al obtener leads (Batch Read): {e}")
        return []


# -------------------- ENGAGEMENTS --------------------
def get_engagements_batch(limit=10000):
    client = get_hubspot_client()
    PROPS = ["hs_email_direction", "hs_timestamp", "hs_from_email", "hs_to_email", "hs_subject", "hs_object_id"]

    try:
        logger.info("📡 Obteniendo lista de IDs de emails...")
        search_request = ObjectSearchRequest(limit=limit, properties=["hs_object_id"])
        search = client.crm.objects.search_api.do_search(object_type="emails", public_object_search_request=search_request)

        email_ids = [r.id for r in search.results]

        if not email_ids:
            logger.info("⚠️ No se encontraron emails.")
            return []

        batch_input = ObjectBatchInput(inputs=[{"id": eid} for eid in email_ids])
        response = client.crm.objects.batch_api.read(
            object_type="emails",
            batch_read_input_simple_public_object_id=batch_input,
            properties=PROPS
        )

        results = response.results
        logger.info(f"⚡ {len(results)} emails obtenidos.")
        if results:
            logger.debug(f"📋 Ejemplo: {results[0].properties}")
        return results

    except ObjectsApiException as e:
        logger.error(f"❌ Error al obtener emails (Batch Read): {e}")
        return []
