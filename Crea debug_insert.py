# debug_insert.py
from utils.hubspot_utils import get_contacts_batch
from utils.db_utils import get_db_connection
from utils.sync_utils import parse_date
import psycopg2

def test_insertion():
    print("🔍 DIAGNÓSTICO DE INSERCIÓN")
    print("=" * 40)
    
    # Obtener datos de HubSpot
    print("1. Obteniendo contactos de HubSpot...")
    contacts = get_contacts_batch(limit=3)
    print(f"   ✅ {len(contacts)} contactos obtenidos")
    
    if not contacts:
        print("   ❌ No hay contactos para insertar")
        return
    
    # Preparar datos para inserción
    print("\n2. Preparando datos para inserción...")
    data = []
    for contact in contacts:
        props = contact.properties
        row = (
            props.get("hs_object_id"),
            props.get("firstname"),
            props.get("lastname"),
            props.get("email"),
            props.get("phone"),
            parse_date(props.get("createdate")),
            parse_date(props.get("lastmodifieddate")),
        )
        data.append(row)
        print(f"   📝 {row}")
    
    # Conectar a la base de datos
    print("\n3. Conectando a la base de datos...")
    conn = get_db_connection(schema="hubspot")
    if not conn:
        print("   ❌ No se pudo conectar a la BD")
        return
    
    try:
        cursor = conn.cursor()
        
        # Verificar que la tabla existe
        print("\n4. Verificando tabla 'contacts'...")
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'hubspot' 
                AND table_name = 'contacts'
            );
        """)
        table_exists = cursor.fetchone()[0]
        print(f"   ✅ Tabla 'contacts' existe: {table_exists}")
        
        if not table_exists:
            print("   ❌ La tabla contacts no existe")
            return
        
        # Intentar inserción manual
        print("\n5. Probando inserción manual...")
        test_query = """
            INSERT INTO hubspot.contacts 
            (hs_object_id, firstname, lastname, email, phone, createdate, lastmodifieddate)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (hs_object_id) DO UPDATE
            SET firstname = EXCLUDED.firstname,
                lastname = EXCLUDED.lastname,
                email = EXCLUDED.email,
                phone = EXCLUDED.phone,
                lastmodifieddate = EXCLUDED.lastmodifieddate;
        """
        
        test_record = data[0]  # Usar el primer registro
        
        print(f"   📋 Ejecutando query con: {test_record}")
        
        cursor.execute(test_query, test_record)
        conn.commit()
        
        print("   ✅ Inserción manual exitosa!")
        
        # Verificar que se insertó
        cursor.execute("SELECT COUNT(*) FROM hubspot.contacts")
        count = cursor.fetchone()[0]
        print(f"   📊 Total de contactos después de inserción: {count}")
        
        # Mostrar el registro insertado
        cursor.execute("SELECT * FROM hubspot.contacts WHERE hs_object_id = %s", (test_record[0],))
        inserted = cursor.fetchone()
        print(f"   📝 Registro insertado: {inserted}")
        
    except psycopg2.Error as e:
        print(f"   ❌ Error de PostgreSQL: {e}")
        conn.rollback()
    except Exception as e:
        print(f"   ❌ Error general: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    test_insertion()