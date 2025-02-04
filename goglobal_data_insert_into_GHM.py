import os
import requests
import xml.etree.ElementTree as ET
from sqlalchemy import MetaData, Table, create_engine, select, update
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import time

load_dotenv()


# Database connection
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')
connection_string = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
# engine = create_engine(connection_string)
engine = create_engine(
    connection_string,
    pool_recycle=1800,  
    pool_pre_ping=True,
    connect_args={"connect_timeout": 30} 
)
metadata = MetaData()
Session = sessionmaker(bind=engine)
session = Session()
global_hotel_mapping = Table("global_hotel_mapping", metadata, autoload_with=engine)



def get_a_column_info(supplier, unica_id):
    query = select(global_hotel_mapping.c[supplier]).where(global_hotel_mapping.c[supplier] == unica_id)
    result = session.execute(query).scalar()
    if result is None:
        print(f"DEBUG: No value found in DB for supplier '{supplier}' and ID '{unica_id}'")
    return result


class GataAPI:
    def __init__(self):
        self.base_url = "http://ghgml.giatamedia.com/webservice/rest/1.0/mappings"
        self.headers = {'Authorization': 'Basic Z2lhdGF8bm9mc2hvbi10b3Vycy5jb206Tm9mc2hvbjEyMy4='}

    def get_hotel_data_using_hotel_id(self, supplier_code, hotel_id):
        url = f"{self.base_url}/{supplier_code}/{hotel_id}"
        # print(f"DEBUG: Calling URL: {url}")
        response = requests.post(url, headers=self.headers)
        if response.status_code == 200:
            try:
                return ET.fromstring(response.text)
            except ET.ParseError as e:
                print("Error parsing XML:", e)
                return None
        print(f"Failed to retrieve data for hotel ID {hotel_id}. Status Code: {response.status_code}")
        return None

    def parse_supplier_codes(self, xml_root):
        supplier_codes = {}
        for code_elem in xml_root.iter('code'):
            supplier = code_elem.attrib.get('supplier')
            value_elem = code_elem.find('value')
            code_value = value_elem.text.strip() if value_elem is not None and value_elem.text else ""
            supplier_codes.setdefault(supplier, []).append(code_value)
        return supplier_codes

    def parse_giata_id(self, xml_root):
        item_elem = xml_root.find('.//item')
        return item_elem.attrib.get('giataId') if item_elem is not None else None

    def get_data(self, supplier_code, hotel_id):
        """
        Fetches hotel data and extracts giataId and supplier codes.
        """
        xml_root = self.get_hotel_data_using_hotel_id(supplier_code, hotel_id)
        if xml_root is None:
            print("DEBUG: xml_root is None, ...................................................................skipping further parsing")
            return None, None

        giata_id = self.parse_giata_id(xml_root)
        provider_data = self.parse_supplier_codes(xml_root)
        # print(f"DEBUG: giata_id: {giata_id}")
        # print(f"DEBUG: provider_data: {provider_data}")

        return giata_id, provider_data



def update_global_hotel_mapping(supplier, unica_id):
    print(f"DEBUG: Starting update for {unica_id}")
    
    hotel_data = get_a_column_info(supplier, unica_id)
    if hotel_data is None:
        print(f"Skipping update for {unica_id} because hotel_data is None")
        return

    gata_api = GataAPI()
    giata_id, provider_records = gata_api.get_data(supplier, hotel_data)

    if giata_id is None or provider_records is None:
        print(f"Skipping update for {unica_id} due to missing giata_id or provider_records")
        return

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

    values_to_update = {col: None for cols in provider_mappings.values() for col in cols}

    for provider_code, ids in provider_records.items():
        if provider_code in provider_mappings:
            provider_columns = provider_mappings[provider_code]
            for i, provider_id in enumerate(ids):
                if i >= len(provider_columns):  # Avoid index out of range
                    break  

                column_name = provider_columns[i]
                current_val = getattr(existing_record, column_name, None)

                # Ensure we don't store duplicates
                if current_val is None or current_val.strip() == "":
                    values_to_update[column_name] = provider_id
                elif str(provider_id) not in str(current_val).split():
                    for col in provider_columns:
                        if not getattr(existing_record, col, None):
                            values_to_update[col] = provider_id
                            break

    try:
        existing_record = session.query(global_hotel_mapping).filter(global_hotel_mapping.c[supplier] == unica_id).first()
    except Exception as e:
        print(f"Error fetching existing record for {unica_id}: {e}")
        return

    final_update_values = {}
    if existing_record and not existing_record.GiataCode:
        final_update_values["GiataCode"] = giata_id
        final_update_values["mapStatus"] = "G-Done"

    for column, value in values_to_update.items():
        if value is not None:
            final_update_values[column] = value

    if final_update_values:
        query = update(global_hotel_mapping).where(global_hotel_mapping.c[supplier] == unica_id).values(**final_update_values)
        
        try:
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    session.execute(query)
                    session.commit()
                    print(f"Successful update: {unica_id}")
                    break 
                except Exception as e:
                    print(f"Commit attempt {attempt + 1} failed for {unica_id}: {e}")
                    session.rollback()
                    if attempt < max_attempts - 1:
                        time.sleep(2)
                    else:
                        print(f"Failed to commit after {max_attempts} attempts for {unica_id}")
        except Exception as e:
            print(f"Error during update execution for {unica_id}: {e}")
            session.rollback()
    else:
        print(f"No changes made for: {unica_id}")



def initialize_tracking_file(file_path, systemid_list):
    if not os.path.exists(file_path):
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(map(str, systemid_list)) + "\n")
    else:
        print(f"Tracking file already exists: {file_path}")


def read_tracking_file(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        return {line.strip() for line in file if line.strip()}


def write_tracking_file(file_path, remaining_ids):
    try:
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(remaining_ids) + "\n")
    except Exception as e:
        print(f"Error writing to tracking file: {e}")


def append_to_cannot_find_file(file_path, systemid):
    try:
        with open(file_path, "a", encoding="utf-8") as file:
            file.write(systemid + "\n")
    except Exception as e:
        print(f"Error appending to 'Cannot find any data' file: {e}")


def update_and_save_function(supplier_code, file_path):
    hotel_id_list = list(read_tracking_file(file_path))
    if not hotel_id_list:
        print(f"No hotel Id to process in {file_path}")
        return
    x = 0
    for hotel_id in hotel_id_list[:]:
        try:
            x += 1
            print(x)
            update_global_hotel_mapping(supplier_code, hotel_id)
            hotel_id_list.remove(hotel_id)
            write_tracking_file(file_path, hotel_id_list)
        except Exception as e:
            print(f"Error processing hotel {hotel_id}: {e}")
            append_to_cannot_find_file(
                f"D:/Rokon/ittImapping_project/static/file/cannot_find_{supplier_code}_hotel_id_list.txt",
                hotel_id
            )
            hotel_id_list.remove(hotel_id)
            write_tracking_file(file_path, hotel_id_list)
    print("Processing completed successfully.")


# Execution
supplier_code = "goglobal"
file_path = f"D:/Rokon/ittImapping_project/static/file/supplier_{supplier_code}_hotel_id_list.txt"
update_and_save_function(supplier_code, file_path)
