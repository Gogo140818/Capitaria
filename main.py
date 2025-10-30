from utils.hubspot_utils import (
    get_contacts_batch, get_deals_batch, get_leads_batch, get_engagements_batch
)
from utils.sync_utils import (
    save_contacts_to_db, save_deals_to_db, save_leads_to_db, save_engagements_to_db
)
from utils.db_utils import (
    init_schema, init_contacts_table, init_deals_table, init_leads_table, init_engagements_table
)

def main():
    print("🧱 Verificando estructura...")
    init_schema("hubspot")
    init_contacts_table("hubspot")
    init_deals_table("hubspot")
    init_leads_table("hubspot")
    init_engagements_table("hubspot")

    # CONTACTOS
    print("\n📇 Descargando contactos (Batch Read)...")
    contacts = get_contacts_batch(limit=10000)
    save_contacts_to_db(contacts)

    # DEALS
    print("\n💼 Descargando deals (Batch Read)...")
    deals = get_deals_batch(limit=10000)
    save_deals_to_db(deals)

    # LEADS
    print("\n👥 Descargando leads (Batch Read)...")
    leads = get_leads_batch(limit=10000)
    save_leads_to_db(leads)

    # ENGAGEMENTS
    print("\n📩 Descargando engagements (Batch Read)...")
    emails = get_engagements_batch(limit=10000)
    save_engagements_to_db(emails)

    print("\n✅ Sincronización completa con Batch Read.")

if __name__ == "__main__":
    main()
