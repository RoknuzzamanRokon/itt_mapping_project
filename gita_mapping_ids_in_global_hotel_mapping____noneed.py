import requests
import xml.etree.ElementTree as ET
from sqlalchemy import MetaData, Table, create_engine, update
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

# Database connection
connection_string = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(connection_string)
metadata = MetaData()
Session = sessionmaker(bind=engine)
session = Session()

global_hotel_mapping = Table("global_hotel_mapping", metadata, autoload_with=engine)

def get_a_column_info(unica_id):
    query = (
        global_hotel_mapping
        .select()
        .with_only_columns(global_hotel_mapping.c.restel)
        .where(global_hotel_mapping.c.VervotechId == unica_id)
    )
    result = session.execute(query).scalar()  
    return result

class GataAPI:
    def __init__(self):
        self.base_url = "https://multicodes.giatamedia.com/webservice/rest/1.0/properties/gds/restel/"
        self.headers = {
            'Authorization': 'Basic Z2lhdGF8bm9mc2hvbi10b3Vycy5jb206Tm9mc2hvbjEyMy4='
        }
    
    def get_hotel_data_using_hotel_id(self, hotel_id):
        url = f"{self.base_url}{hotel_id}"
        response = requests.post(url, headers=self.headers)
        
        if response.status_code == 200:
            root = ET.fromstring(response.text)
            return root
        else:
            print(f"Failed to retrieve data for hotel ID {hotel_id}. Status Code: {response.status_code}")
            return None
    
    def get_data(self, hotel_id):
        root = self.get_hotel_data_using_hotel_id(hotel_id)
        
        if root is not None:
            property_element = root.find(".//property")
            if property_element is not None:
                giata_id = property_element.get("giataId")
                # hotel_name = property_element.find("name").text if property_element.find("name") is not None else None
                # print(hotel_name)
                # city_element = property_element.find("city")
                # city_id = city_element.get("cityId") if city_element is not None else None
                # print(city_id)

                provider_data = {}
                for provider in root.findall(".//propertyCodes/provider"):
                    provider_code = provider.get("providerCode")
                    provider_values = [code.text for code in provider.findall("code/value")]
                    provider_data[provider_code] = provider_values
                
                return giata_id, provider_data
            else:
                return None, {}  
        else:
            return None, {} 

def update_global_hotel_mapping(unica_id):
    # Fetch hotel ID and data from API
    hotel_data = get_a_column_info(unica_id)
    gata_api = GataAPI()
    giata_id, provider_records = gata_api.get_data(hotel_data)

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

    # Initialize values to update
    values_to_update = {key: None for sublist in provider_mappings.values() for key in sublist}

    for provider_code, ids in provider_records.items():
        if provider_code in provider_mappings:
            for column_name, provider_id in zip(provider_mappings[provider_code], ids):
                if column_name in values_to_update and not values_to_update[column_name]:
                    values_to_update[column_name] = provider_id


    existing_record = session.query(global_hotel_mapping).filter(global_hotel_mapping.c.VervotechId == unica_id).first()

    # Prepare the final update values
    final_update_values = {}
    
    # Check for GiataCode and update it
    if existing_record and not existing_record.GiataCode:
        final_update_values["GiataCode"] = giata_id
    
    # Always update mapStatus when GiataCode is updated or inserted
    if 'GiataCode' in final_update_values:
        final_update_values["mapStatus"] = "G-Done"
    
    for column, value in values_to_update.items():
        if value is not None:
            existing_value = getattr(existing_record, column, None)
            if existing_value in (None, ''): 
                final_update_values[column] = value
    
    if final_update_values:
        query = (
            update(global_hotel_mapping)
            .where(global_hotel_mapping.c.VervotechId == unica_id)
            .values(**final_update_values)
        )
        session.execute(query)
        session.commit()
        print(f"Successful update: {unica_id}")
    else:
        print(f"No changes made for: {unica_id}")

    
    
# Example usage
unica_id = "38944169"
update_global_hotel_mapping(unica_id)
