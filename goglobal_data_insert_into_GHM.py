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





