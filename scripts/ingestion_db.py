import pandas as pd
import os
from sqlalchemy import create_engine
import time
import logging


logging.basicConfig(
    filename = r"C:\Users\KR\Downloads\video\New folder\projects\data\data\logs\ingestion_db.log",
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    filemode = "a"  
)


engine = create_engine(r'sqlite:///inventory.db')


def ingest_db(df,table_name,engine):
    ''' This function will ingest the dataframe into db table '''
    df.to_sql(table_name, con = engine, if_exists = 'replace', index = False)

    
def load_raw_data():
    ''' This function will load CSVs as dataframe and will ingest in db  '''
    start = time.time()
    for file in os.listdir(r'C:\Users\KR\Downloads\video\New folder\projects\data\data'):
        if '.csv' in file:
            df = pd.read_csv(r'C:\Users\KR\Downloads\video\New folder\projects\data\data/'+file)
            logging.info(f'ingesting {file} in db ')
            ingest_db(df, file[:-4], engine)
    end = time.time()
    total_time = (end - start)/60
    logging.info('----Ingestion Completed-------')
    logging.info(f'\nTotal time taken {total_time} minutes')
            
if __name__ == '__main__':
    load_raw_data()


