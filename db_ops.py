import psycopg2
from psycopg2 import Error
import pandas as pd
import pandas.io.sql as sqlio
import os
from dotenv import load_dotenv
import psycopg2.extras as extras
import warnings
from psycopg2 import sql

warnings.filterwarnings('ignore')


load_dotenv()


def create_conn():
    conn = psycopg2.connect(
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        host=os.getenv('HOST'),
        port=os.getenv('PORT'),
    )

    return conn


def execute_sql(query: str, has_results: bool = False, message: str = 'SQL successfully executed'):
    """
    connect to the postgres DB and execute the SQL statement. Return a dataframe if the SQL to be executed returns results.
    """
    try:
        # Establish a connection to the Postgres database
        conn = create_conn()
        cur = conn.cursor()

        if has_results:
            # return query results as a dataframe
            df = sqlio.read_sql_query(query, conn)

            return df

        else:
            # execute the SQL statement and commit
            # create a cursor
            cur.execute(query)
            conn.commit()

    except (Exception, Error) as error:
        print('Error: ', error)
        cur.close()
        conn.close()

    finally:
        # Close the database connection
        cur.close()
        conn.close()
        if not has_results:
            print(message)


def create_table(schema: str, table: str, columns: dict, drop_first: bool = False):
    """
    create a table with the given table name and data structure 
    """
    if drop_first:
        execute_sql(
            f'DROP TABLE IF EXISTS {schema}.{table}', message=f"{table} table dropped if existed")

    execute_sql(
        query=f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table}
            ({", ".join(f"{colname} {data_type}" for colname, data_type in columns.items())})
        """,
        message=f"{table} table created/already exists"
    )


def df_to_table(df: pd.DataFrame, schema: str, table: str, incremental: bool = True, unique_col: str = None):
    """
    Using psycopg2.extras.execute_values() to insert a dataframe. The table must have columns that match all the 
    columns of the df
    """
    if incremental:
        existing = execute_sql(f"SELECT {unique_col} FROM {schema}.{table}", has_results=True)
        if len(existing) > 0:
            df = df.merge(existing, how='left',indicator=True)
            df = df[df['_merge']=='left_only']
            df.drop('_merge', axis=1, inplace=True)

    if len(df) > 0:
        # Create a list of tupples from the dataframe values
        data = [tuple(x) for x in df.to_numpy()]
        
        # SQL query to execute
        query = sql.SQL("insert into {}.{} VALUES %s").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            )

        conn = create_conn()
        cur = conn.cursor()

        try:
            extras.execute_values(cur, query, data)
            conn.commit()

        except (Exception, Error) as error:
            print("Error: %s" % error)
            conn.rollback()

        finally:
            cur.close()
            conn.close()
        print(f"data insert into {table} table successful")

    else:
        print(f'All records already exist for {table} table!')