from dotenv import load_dotenv
from os import getenv
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_batch, execute_values
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

class PgHook:
    def __init__(self):
        env_path = Path(__file__).parent.parent / 'secrets' / '.env'
        load_dotenv(env_path)
        self.__pgurl = getenv("POSTGRES_URL")
        self.__user = getenv("POSTGRES_USER")
        self.__pword = getenv("POSTGRES_PASSWORD")
        self.__dbname = "quant01"

        # create SQLAlchemy engine
        self.engine = create_engine(self.__pgurl)
        
    def __repr__(self):
        return "Class handling queries and table creates with the Postges server"
    
    def get_psycopg_connection(self):
        """Open a raw psycopg2 connection using stored env vars."""
        return psycopg2.connect(
            dbname=self.__dbname,
            user=self.__user,
            password=self.__pword,
            host=getenv("POSTGRES_HOST", "localhost"),
            port=getenv("POSTGRES_PORT", 5432)
            )
    
    def execute_sql(self, sql, params=None, commit=True):
        """Execute SQL command directly using psycopg2."""
        with self.get_psycopg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
            if commit:
                conn.commit()

    def bulk_insert(self, in_df, table_name):
        """Insert a DataFrame efficiently using psycopg2 execute_batch."""
        if in_df.empty:
            return
        
        cols = ', '.join(in_df.columns)
        sql = f"INSERT INTO {table_name} ({cols}) VALUES %s"

        # Convert DataFrame to list of tuples with Python native types
        # Replace NaN with None for proper NULL handling
        data = in_df.replace({np.nan: None}).values.tolist()

        with self.get_psycopg_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, data, page_size=2000)
            conn.commit()

    def psy_query(self, sql):
        """Execute SQL to query database and return results as a Dataframe."""
        with self.get_psycopg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                # get results to list
                result = cur.fetchall()
                # get column names for query result
                col_names = [desc[0] for desc in cur.description]                

        return pd.DataFrame(result, columns=col_names)

    # --- SQLAlchemy Methods ---
    def alc_query(self, sql):
        ''' Simple query method '''
        return pd.read_sql_query(sql, self.engine)

    def alc_df_2_db_r(self, in_df, tablename):
        ''' Method to move dataframe to Postgres table via REPLACE'''
        # load dataframe to test table
        in_df.to_sql(tablename, self.engine, if_exists="replace", index=False)

    def alc_df_2_db_a(self, in_df, tablename):
        ''' Method to move dataframe to Postgres table via APPEND'''
        # load dataframe to test table
        in_df.to_sql(tablename, self.engine, if_exists="append", index=False)

    def alc_exec_sql(self, sql: str, params=None):
        """Execute SQL command (for moves, updates, etc.)"""
        with self.engine.begin() as conn:
            if params is None:
                conn.execute(text(sql))
            else:
                conn.execute(text(sql), params)
