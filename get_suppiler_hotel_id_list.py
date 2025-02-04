from sqlalchemy import MetaData, Table, create_engine, select, update
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

def get_unique_id_list(supplier):
    query = (
        select(global_hotel_mapping.c[supplier])
        .distinct()
        .where(global_hotel_mapping.c[supplier].isnot(None))
    )
    result = session.execute(query).scalars().all()
    return result
    
def save_id_list_to_file(supplier):
    unique_ids = get_unique_id_list(supplier)
    file_name = f"D:/Rokon/ittImapping_project/static/file/{supplier}_supplier__hotel_id_list_2.txt"
    
    with open(file_name, "w") as file:
        for unica_id in unique_ids:
            file.write(f"{unica_id}\n")
    print(len(unique_ids))
    print(f"Unique IDs saved to {file_name}")

supplier = "grnconnect"
save_id_list_to_file(supplier)