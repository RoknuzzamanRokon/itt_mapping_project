from sqlalchemy import MetaData, Table, create_engine, select, update, func
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

load_dotenv()

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

connection_string = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(connection_string)
metadata = MetaData()
Session = sessionmaker(bind=engine)
session = Session()

global_hotel_mapping = Table("global_hotel_mapping_2", metadata, autoload_with=engine)



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
    "ratehawk2": ["ratehawkhotel", "ratehawkhotel_a", "ratehawkhotel_b", "ratehawkhotel_c", "ratehawkhotel_d", "ratehawkhotel_e"],
    "adivahotel": ["adivahahotel", "adivahahotel_a", "adivahahotel_b", "adivahahotel_c", "adivahahotel_d", "adivahahotel_e"],
    "grnconnect": ["grnconnect", "grnconnect_a", "grnconnect_b", "grnconnect_c", "grnconnect_d", "grnconnect_e"],
    "juniper": ["juniperhotel", "juniperhotel_a", "juniperhotel_b", "juniperhotel_c", "juniperhotel_d", "juniperhotel_e"],
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


def get_unique_id_list(supplier):
    columns = provider_mappings.get(supplier, [supplier])  
    query = select(*[global_hotel_mapping.c[col] for col in columns]) 
    result = session.execute(query).all()  
    
    unique_ids = {row[col] for row in result for col in range(len(columns)) if row[col] is not None}

    return list(unique_ids)  

def save_id_list_to_file(supplier):
    unique_ids = get_unique_id_list(supplier)
    file_name = f"D:/Rokon/ittImapping_project/static/file/{supplier}_supplier_hotel_id_list_3.txt"
    
    with open(file_name, "w") as file:
        for unique_id in unique_ids:
            file.write(f"{unique_id}\n")

    print(len(unique_ids))
    print(f"Unique IDs saved to {file_name}")

supplier = "grnconnect"
save_id_list_to_file(supplier)
