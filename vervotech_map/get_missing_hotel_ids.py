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
engine = create_engine(connection_string)

metadata = MetaData()
Session = sessionmaker(bind=engine)
session = Session()

global_hotel_mapping = Table("global_hotel_mapping", metadata, autoload_with=engine)
vervotech_mapping = Table("vervotech_mapping", metadata, autoload_with=engine)


def get_vervotech_mapping_table_all_vervotech_id(session):
    query = session.query(vervotech_mapping.c.VervotechId).all()
    data = [row.VervotechId for row in query]
    return data

def get_global_hotel_table_all_vervotech_id(session):
    query = session.query(global_hotel_mapping.c.VervotechId).all()
    data = [row.VervotechId for row in query]
    return data

def save_results(a, b):
    set_a = set(map(str, a))
    set_b = set(map(str, b))

    result = set_b - (set_a & set_b)

    results = {"All_get_data_for_new_and_missing_file.txt": result}

    for filename, data in results.items():
        with open(filename, 'w') as f:
            f.write("\n".join(data))

    print("Results saved to respective files.")


if __name__ == "__main__":
    try:
        global_vervotech_id_list = get_global_hotel_table_all_vervotech_id(session=session)

        vervotech_mapping_id_list = get_vervotech_mapping_table_all_vervotech_id(session=session)
        save_results(a=global_vervotech_id_list, b=vervotech_mapping_id_list)
    finally:
        session.close()