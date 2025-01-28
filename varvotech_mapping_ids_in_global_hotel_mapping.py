from sqlalchemy import MetaData, Table, create_engine, update
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

load_dotenv()

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

connection_string = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
# connection_string = "mysql+pymysql://root:@localhost/innova_db_v1.25"

engine = create_engine(connection_string)

metadata = MetaData()  
Session = sessionmaker(bind=engine)
session = Session()

global_hotel_mapping = Table("global_hotel_mapping", metadata, autoload_with=engine)
vervotech_mapping = Table("vervotech_mapping_2", metadata, autoload_with=engine)



def get_a_column_info_follow_a_id(unica_id):
    query = (
        vervotech_mapping
        .select()
        .with_only_columns(vervotech_mapping.c.ProviderHotelId, vervotech_mapping.c.ProviderFamily) 
        .where(vervotech_mapping.c.UnicaId == unica_id)
    )
    result = session.execute(query).mappings().all()
    return result

def update_global_hotel_mapping(unica_id):
    records = get_a_column_info_follow_a_id(unica_id)

    # Define provider mappings for families
    provider_mappings = {
            "hotelbeds": ["hotelbeds", "hotelbeds_a", "hotelbeds_b", "hotelbeds_c", "hotelbeds_d", "hotelbeds_e"],
            "agoda": ["agoda", "agoda_a", "agoda_b", "agoda_c", "agoda_d", "agoda_e"],
            "tbo": ["tbohotel", "tbohotel_a", "tbohotel_b", "tbohotel_c", "tbohotel_d", "tbohotel_e"],
            "ean": ["ean", "ean_a", "ean_b", "ean_c", "ean_d", "ean_e"],
            "mgholiday": ["mgholiday", "mgholiday_a", "mgholiday_b", "mgholiday_c", "mgholiday_d", "mgholiday_e"],
            "restel": ["restel", "restel_a", "restel_b", "restel_c", "restel_d", "restel_e"],
            "stuba": ["stuba", "stuba_a", "stuba_b", "stuba_c", "stuba_d", "stuba_e"],
            "hyperguestdirect": ["hyperguestdirect", "hyperguestdirect_a", "hyperguestdirect_b", "hyperguestdirect_c", "hyperguestdirect_d", "hyperguestdirect_e"],
            "goglobal": ["goglobal", "goglobal_a", "goglobal_b", "goglobal_c", "goglobal_d", "goglobal_e"],
            "ratehawk": ["ratehawkhotel", "ratehawkhotel_a", "ratehawkhotel_b", "ratehawkhotel_c", "ratehawkhotel_d", "ratehawkhotel_e"],
            "adivahotel": ["adivahahotel", "adivahahotel_a", "adivahahotel_b", "adivahahotel_c", "adivahahotel_d", "adivahahotel_e"],
            "grnconnect": ["grnconnect", "grnconnect_a", "grnconnect_b", "grnconnect_c", "grnconnect_d", "grnconnect_e"],
            "juniperhotel": ["juniperhotel", "juniperhotel_a", "juniperhotel_b", "juniperhotel_c", "juniperhotel_d", "juniperhotel_e"],
            "mikihotel": ["mikihotel", "mikihotel_a", "mikihotel_b", "mikihotel_c", "mikihotel_d", "mikihotel_e"],
            "paximumhotel": ["paximumhotel", "paximumhotel_a", "paximumhotel_b", "paximumhotel_c", "paximumhotel_d", "paximumhotel_e"],
            "adonishotel": ["adonishotel", "adonishotel_a", "adonishotel_b", "adonishotel_c", "adonishotel_d", "adonishotel_e"],
            "w2mhotel": ["w2mhotel", "w2mhotel_a", "w2mhotel_b", "w2mhotel_c", "w2mhotel_d", "w2mhotel_e"],
            "oryxhotel": ["oryxhotel", "oryxhotel_a", "oryxhotel_b", "oryxhotel_c", "oryxhotel_d", "oryxhotel_e"],
            "dotw": ["dotw", "dotw_a", "dotw_b", "dotw_c", "dotw_d", "dotw_e"],
            "hotelston": ["hotelston", "hotelston_a", "hotelston_b", "hotelston_c", "hotelston_d", "hotelston_e"],
            "letsflyhotel": ["letsflyhotel", "letsflyhotel_a", "letsflyhotel_b", "letsflyhotel_c", "letsflyhotel_d", "letsflyhotel_e"],
            "illusionshotel": ["illusionshotel"]
            }

    values_to_update = {key: None for sublist in provider_mappings.values() for key in sublist}

    for record in records:
        provider_family = record["ProviderFamily"].lower()
        
        if provider_family in provider_mappings:
            for key in provider_mappings[provider_family]:
                if not values_to_update[key]:
                    values_to_update[key] = record["ProviderHotelId"]
                    break  

    # Prepare and execute the update query
    query = (
        update(global_hotel_mapping)
        .where(global_hotel_mapping.c.VervotechId == unica_id)
        .values(**values_to_update, mapStatus="Done")  
    )

    session.execute(query)
    session.commit()
    print(f"Successful update: {unica_id}")


def initialize_tracking_file(file_path, systemid_list):
    """
    Initializes the tracking file with all SystemIds if it doesn't already exist.
    """
    if not os.path.exists(file_path):
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(map(str, systemid_list)) + "\n")
    else:
        print(f"Tracking file already exists: {file_path}")


def read_tracking_file(file_path):
    """
    Reads the tracking file and returns a set of remaining SystemIds.
    """
    with open(file_path, "r", encoding="utf-8") as file:
        return {line.strip() for line in file.readlines()}


def write_tracking_file(file_path, remaining_ids):
    """
    Updates the tracking file with unprocessed SystemIds.
    """
    try:
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(remaining_ids) + "\n")
    except Exception as e:
        print(f"Error writing to tracking file: {e}")


def append_to_cannot_find_file(file_path, systemid):
    """
    Appends the SystemId to the 'Cannot find any data' tracking file.
    """
    try:
        with open(file_path, "a", encoding="utf-8") as file:
            file.write(systemid + "\n")
    except Exception as e:
        print(f"Error appending to 'Cannot find any data' file: {e}")



def update_and_save_function(file_path):
    vervotech_id_list = read_tracking_file(file_path)
    
    if not vervotech_id_list:
        print(f"No Vervotech Id to process in {file_path}")
        return

    vervotech_id_list = list(vervotech_id_list)  # Convert set to list

    index = 0
    while index < len(vervotech_id_list):
        vervotech_id = vervotech_id_list[index]
        try:
            update_global_hotel_mapping(vervotech_id)
            
            # Remove the processed Vervotech Id from the list
            vervotech_id_list.pop(index)
            
            write_tracking_file(file_path, vervotech_id_list)

        except Exception as e:
            print(f"Error processing Vervotech {vervotech_id}: {e}")
            append_to_cannot_find_file("cannot_find_file.txt", vervotech_id)

file = "D:/Rokon/ittImapping_project/id_list_file.txt"
update_and_save_function(file)


