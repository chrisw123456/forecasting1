import pymssql
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from pymssql import Connection
import pandas as pd


# sql server engine func
def get_engine(username:str, password:str, server:str, database:str) -> Engine:
    """ connects to MS SQL Server
    Args:
        username (str)
        password (str)
        server (str)
        database (str)
    Returns:
        Engine
    """
    conn_string = f"mssql+pymssql://{username}:{password}@{server}/{database}"
    engine = create_engine(conn_string)
    return engine

# get connection
def get_cnx(username:str, password:str, server:str, database:str) -> Connection:
    """ enables querying MS SQL Server
    Args:
        username (str): [description]
        password (str): [description]
        server (str): [description]
        database (str): [description]
    Returns:
        pymssql.Connection: [description]
    """
    cnx = pymssql.connect(user=username, password=password,
                          host=server, database=database)
    return cnx

# read table to pandas
def read_sql_table(table_name:str, cnx:pymssql.Connection) -> pd.DataFrame:
    """reads sql table into pandas df
    Args:
        table_name (str): table name in SQL DB
        cnx (pymssql.Connection): connection object used to read query
    Returns:
        pd.DataFrame: [description]
    """
    sql_table = pd.read_sql(sql=f"SELECT * FROM {table_name}",
                            con=cnx)

    return sql_table

# write df tp sql server
def write_df_to_sql(
        expID: str, new_df: pd.DataFrame, table_name: str, cnx: Connection, engine: Engine,
        replace_exist_table=False):
    """[summary]
    #columns_to_consider: List[str]
    Args:
        new_df (pd.DataFrame): name of df to be written to sql db
        table_name (str): name of table to write to 
        cnx (Connection): used to read sql tables
        engine (Engine): used to write to sql db 
        columns_to_consider (List[str]): columns to be examined when checking for duplicates
        init (bool, optional): indicates if tables need 
        to be created for the first time. Defaults to False.
    """
    # add expID
    new_df.insert(0,'ExpID',str(expID))

    # conert all columns to string
    new_df=new_df.applymap(str)

    # convert all columns named with (min) to _min
    new_df.columns=[i.replace('(min)','_min') if '(min)' in i else i for i in new_df.columns]
    
    #write new_df to sql server
    if replace_exist_table:
        if_exists='replace'
    
    else:  
        if_exists='append'

    new_df.to_sql(name=table_name,con=engine, index=False, if_exists=if_exists)
    
