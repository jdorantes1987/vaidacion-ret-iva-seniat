from pandas import DataFrame
from pandas import read_sql_query
from pandas import NA
from sqlalchemy import text

def get_read_sql(sql, conexion) -> DataFrame:  
    try:
        conex = conexion
        engine = conex.c_engine()
        df = read_sql_query(text(sql), engine)
    except Exception as e:
        df = NA
        print("Ocurrió un error al consultar: ", e)
    return df
