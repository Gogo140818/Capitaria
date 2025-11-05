from utils.hubspot_utils import get_all_contacts
from utils.sync_utils import save_contacts_to_db
from utils.db_utils import init_schema, init_contacts_table

def main():
    print("🚀 INICIANDO SINCRONIZACIÓN COMPLETA DE CONTACTOS")
    print("=" * 50)
    
    # 1. Inicializar estructura
    print("\n1. 🗄️  Inicializando base de datos...")
    init_schema("hubspot")
    init_contacts_table("hubspot")
    print("✅ Estructura verificada")

    # 2. Obtener TODOS los contactos
    print("\n2. 📡 Obteniendo TODOS los contactos de HubSpot...")
    contacts = get_all_contacts()
    print(f"📊 Contactos obtenidos: {len(contacts)}")
    
    if not contacts:
        print("❌ No se pudieron obtener contactos")
        return

    # 3. Guardar en base de datos
    print("\n3. 💾 Guardando contactos en base de datos...")
    save_contacts_to_db(contacts)

    print("\n🎊 SINCRONIZACIÓN COMPLETADA!")

if __name__ == "__main__":
    main()