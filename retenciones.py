from pandas import read_excel
from os import getenv
from dotenv import load_dotenv

class RetencionesIVA:
    def __init__(self, empresa):
        if empresa == getenv('DB_NAME_PROFIT_PANA'):
            self.data_retenciones =  read_excel('RETENCIONES panaraca.xlsx', dtype={'Nro.Documento':'str'})
        else:
            self.data_retenciones =  read_excel('RETENCIONES doel.xlsx', dtype={'Nro.Documento':'str'})
            
    def retenciones_clientes(self):
        return self.data_retenciones
    
    def set_doc_facturas_retenciones_iva(self):
        return set(self.data_retenciones['Nro.Documento'])
    
            
if __name__ == '__main__':
    load_dotenv()
    print(RetencionesIVA('DPANA_A').retenciones_clientes())