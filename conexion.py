import os
import pyodbc
from sqlalchemy.engine import URL
from sqlalchemy import create_engine

#  Valores por defecto
args = {
    "driver": "mssql+pyodbc",
    "proveedor": "{SQL Server}",
    "tipo_c": "odbc_connect",
    "host": os.getenv("HOST_PRODUCCION_PROFIT"),
    "puerto": "27017",
    "usuario": os.getenv("DB_USER_PROFIT"),
    "pword": os.getenv("DB_PASSWORD_PROFIT"),
    "base_de_datos": os.getenv("DB_NAME_PROFIT_DOEL")
}

class ConexionBD:
    def __init__(self, **kwargs):
        self.driver = kwargs.get('driver', args['driver'])
        self.proveedor = kwargs.get('proveedor', args['proveedor'])
        self.tipo_con = kwargs.get('tipo_c', args['tipo_c'])
        self.servidor = kwargs.get('host', args['host'])
        self.bddatos = kwargs.get('base_de_datos', args['base_de_datos'])
        self.usuario = kwargs.get('usuario', args['usuario'])
        self.clave = kwargs.get('pword', args['pword'])
        self.conn = None

    def conectar(self):
        try:
            str_conn = "DRIVER={prov};SERVER={host};DATABASE={db};UID={user};PWD={pw}".format(
                                                                                              prov=self.proveedor, 
                                                                                              host=self.servidor,
                                                                                              db=self.bddatos, 
                                                                                              user=self.usuario, 
                                                                                              pw=self.clave
                                                                                              )
            self.conn = pyodbc.connect(str_conn)
            # print("Conexión exitosa a la base de datos.")
        except pyodbc.Error as e:
            print(f"Error al conectar a la base de datos: {e}")

    def desconectar(self):
        if self.conn:
            self.conn.close()
            # print("Conexión cerrada.")
            
    def c_engine(self):
        con_str = f'DRIVER={self.proveedor};SERVER={self.servidor};DATABASE={self.bddatos};UID={self.usuario};PWD={self.clave}'
        connection_url = URL.create(self.driver, query={self.tipo_con: con_str})
        return create_engine(connection_url)
    
    def iniciar_transaccion(self):
        self.conn.autocommit = False
    
    def confirmar_transaccion(self):
        self.conn.commit()
        self.conn.autocommit = True

    def revertir_transaccion(self):
        self.conn.rollback()
        self.conn.autocommit = True
    
            
# # Ejemplo de uso:
# conexion = ConexionBD()
# conexion.conectar()
# Realiza operaciones en la base de datos...
# conexion.desconectar()
