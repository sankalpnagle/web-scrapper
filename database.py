from datetime import datetime, timedelta
#import cx_Oracle
import psycopg2
# from sqlalchemy import create_engine
#cx_Oracle.init_oracle_client(lib_dir=r"C:\OracleApp\instantclient_19_19")
import os

#os.environ["LD_LIBRARY_PATH"] = "/opt/oracle/instantclient_19_20"

#cx_Oracle.init_oracle_client(lib_dir= r"/opt/oracle/instantclient_19_20")

# def cirrusConnection():
#    # Database connection
#    connection = cx_Oracle.connect('cirrus/Cir^Pnq$6A@54.38.215.111/cirrus')
#    return connection


# def localConnection():
#    ps_connecction = psycopg2.connect(
#       database="cirrus_local",
#       user="local",
#       password="Cir^Pnq@2023",
#       host="51.195.235.59",
#       port="5432"
#    )
#    return ps_connecction

# def cirrusEngine():
#    # db_username = 'cirrus'
#    # db_password = 'Cir^Pnq@6A'
#    # db_host = '54.38.215.111'
#    # db_port = '1521'
#    # db_service_name = 'cirrus'
#    # # Construct the SQLAlchemy connection string
#    # connection_string = f"oracle+cx_oracle://{db_username}:{db_password}@{db_host}:{db_port}/{db_service_name}"
#    # connection_string = f"oracle+cx_oracle://{db_username}:{db_password}@{db_host}/{db_service_name}"
#    # # connection_string = f"oracle+cx_oracle://{db_username}:{db_password}@{db_host}:{db_port}/{db_sid}"


#    # # Create an SQLAlchemy engine
#    # engine = create_engine(connection_string)
   
#    # Replace your existing cx_Oracle connection string
#    cx_oracle_connection_string = 'cirrus/Cir^Pnq$6A@54.38.215.111/cirrus'

#    # Manually extract components
#    username, password_host_sid = cx_oracle_connection_string.split('/', 1)
#    username, password_host = username.split('@', 1)
#    password, host_sid = password_host_sid.split('@', 1)
#    host, sid = host_sid.split('/', 1)

#    # Construct SQLAlchemy connection string using SID
#    sqlalchemy_connection_string = f"oracle+cx_oracle://{username}:{password}@{host}/{sid}"

#    # Create an SQLAlchemy engine
#    engine = create_engine(sqlalchemy_connection_string)
      
#    return engine



def localConnection():
   ps_connection = psycopg2.connect(
      database="dev_cirrus",
      user="prod_cirrus",
      password="Cir^Pnq@2025",
      #host="51.195.235.59",
      host="148.113.44.129",
      port="15324"
      )
   return ps_connection

# from pymongo import MongoClient
# def mongoConnection():
#    # mongoClient = MongoClient('mongodb://admin:Cir%5EPnq%246A@51.195.235.59:27017/?authMechanism=DEFAULT')
#    # mongodb://admin:Cir%5EPnq%246A@57.128.169.41:27017/
#    mongoClient = MongoClient('mongodb://admin:Cir%5EPnq%246A@57.128.169.41:27017/?authMechanism=DEFAULT')
#    return mongoClient