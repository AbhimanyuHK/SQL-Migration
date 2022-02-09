import pandas as pd
import cx_Oracle
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError

USER_NAME = "usr"
PASSWORD = "pswd"
HOST = "localhost"
SERVICE_NAME = "orclpdb1"
SQL_QUERY = """SELECT * FROM orders"""


class DBConfig:

    def __init__(self):
        self.engine = sqlalchemy.create_engine(
            "oracle+cx_oracle://{}:{}@{}/?service_name={}", arraysize=10000
        )


class Migration:

    @staticmethod
    def process():
        for df in pd.read_sql(SQL_QUERY, con=DBConfig().engine, chunksize=10000):
            print("Loading : ", df.shape)
            df.to_sql("table_name", con=DBConfig().engine, if_exists="append")


if __name__ == "__main__":
    Migration().process()
