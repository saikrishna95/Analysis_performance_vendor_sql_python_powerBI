
# Using this script to save CSV files into database with their filename as tablename

# importing required libraries
import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time
import traceback

# adding a logger
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# creating a database engine
engine = create_engine("sqlite:///inventory1.db")


def ingest_db(df, table_name, engine):
    """
    This function will ingest the dataframe into a database table
    """
    df.to_sql(
        table_name,
        con=engine,
        if_exists="replace",
        index=False
    )


def load_raw_data():
    """
    This function will load the CSVs as dataframe and ingest them into the DB
    """
    start = time.time()

    for file in os.listdir("data"):
        try:
            if file.endswith(".csv"):
                df = pd.read_csv(os.path.join("data", file))
                logging.info(f"Ingesting {file} into database")
                ingest_db(df, file[:-4], engine)

        except Exception as e:
            logging.error(
                f"------------------- Failed to ingest {file}: {e} -------------------"
            )
            logging.debug(traceback.format_exc())

    end = time.time()
    total_time = (end - start) / 60

    logging.info("-------------- Ingestion Complete ------------")
    logging.info(f"Total Time Taken: {total_time:.2f} minutes")


if __name__ == "__main__":
    load_raw_data()