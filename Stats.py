import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import Error
from pandas.io import sql
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

user="root"
password=""
host="YOUR IP ADDRESS"
database="YOUR DB NAME"

def SQLAlchemyEngine(user, password, host, database):
    engine = create_engine('mysql+mysqlconnector://%s:%s@%s/%s' % (user, password, host, database))
    return engine

def UrlTODataframe(url):
    dataframe = pd.read_csv(url)
    dataframe = dataframe.replace({np.nan: 0})
    dataframe.index = np.arange(1, len(dataframe)+1)
    return dataframe

def SQLAlchemyExe(eng, table_name, url):
    dataframe = UrlTODataframe(url)
    dataframe.to_sql(name=table_name, con=eng, if_exists = 'replace', index = 'id')
    eng.execute('ALTER TABLE %s ADD PRIMARY KEY (`id`);' % (table_name))
    return print("[%s] Done!" % (table_name))

try:
    connString = mysql.connector.connect(user=user, password=password, host=host, database=database)

    if connString.is_connected:
        print("Connected")

        # ENGINE GENERATOR
        engine = SQLAlchemyEngine(user, password, host, database)

        # CASES MALAYSIA
        SQLAlchemyExe(engine, "cases_malaysia", "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/cases_malaysia.csv")

        # CASES STATE
        SQLAlchemyExe(engine, "cases_state", "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/cases_state.csv")

        # CLUSTERS
        SQLAlchemyExe(engine, "clusters", "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/clusters.csv")

        # TESTS MALAYSIA
        SQLAlchemyExe(engine, "tests_malaysia", "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/tests_malaysia.csv")

        # TESTS STATE
        SQLAlchemyExe(engine, "tests_state", "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/tests_state.csv")

        # PKRC
        SQLAlchemyExe(engine, "pkrc", "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/pkrc.csv")

        # HOSPILTAL
        SQLAlchemyExe(engine, "hospital", "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/hospital.csv")

        # ICU
        SQLAlchemyExe(engine, "icu", "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/icu.csv")

        # DEATHS MALAYSIA
        SQLAlchemyExe(engine, "deaths_malaysia", "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/deaths_malaysia.csv")

        # DEATHS STATE
        SQLAlchemyExe(engine, "deaths_state", "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/deaths_state.csv")

        # CHECKIN MALAYSIA
        SQLAlchemyExe(engine, "checkin_malaysia", "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/mysejahtera/checkin_malaysia.csv")

        # CHECKIN STATE
        SQLAlchemyExe(engine, "checkin_state", "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/mysejahtera/checkin_state.csv")

        # CHECKIN MALAYSIA TIME
        SQLAlchemyExe(engine, "checkin_malaysia_time", "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/mysejahtera/checkin_malaysia_time.csv")

        # TRACE MALAYSIA
        SQLAlchemyExe(engine, "trace_malaysia", "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/mysejahtera/trace_malaysia.csv")

        # POPULATION
        SQLAlchemyExe(engine, "population", "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/static/population.csv")

        # VAX MALAYSIA
        SQLAlchemyExe(engine, "vax_malaysia", "https://raw.githubusercontent.com/CITF-Malaysia/citf-public/main/vaccination/vax_malaysia.csv")

        # VAX STATE
        SQLAlchemyExe(engine, "vax_state", "https://raw.githubusercontent.com/CITF-Malaysia/citf-public/main/vaccination/vax_state.csv")

        # VAX REGISTRATION  MALAYSIA
        SQLAlchemyExe(engine, "vaxreg_malaysia", "https://raw.githubusercontent.com/CITF-Malaysia/citf-public/main/registration/vaxreg_malaysia.csv")

        # VAX REGISTRATION  STATE
        SQLAlchemyExe(engine, "vaxreg_state", "https://raw.githubusercontent.com/CITF-Malaysia/citf-public/main/registration/vaxreg_state.csv")

    else:
        print('Unable to connect to DB')

except SQLAlchemyError as e:
    print("[ERROR]:", e)
