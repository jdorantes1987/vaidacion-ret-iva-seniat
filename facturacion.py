from os import getenv
from pandas import read_excel, merge, merge_asof
from dotenv import load_dotenv
from sql_read import get_read_sql
from conexion import ConexionBD
from retenciones import RetencionesIVA

load_dotenv()

class FacturaVentasConsultas:
    def __init__(self, conexion, empresa):
        self.conexion = conexion
        self.retenciones_iva = RetencionesIVA(empresa=empresa)

    def data_estadisticas_bcv(self):
        tasas = read_excel(getenv('FILE_REMOTE_ESTADISTICAS_BCV'))
        return tasas.sort_values(by=['fecha'], ascending=[True]) 
    
    def facturas_ventas_resumen(self, fecha_ini, fecha_fin):
        sql = f"""
                EXEC [RepFacturaVentaxFecha] 
                @dCo_fecha_d = '{fecha_ini}',
                @dCo_fecha_h = '{fecha_fin}'
                """
        ventas_resumen = get_read_sql(sql, self.conexion)
        ventas_resumen['doc_num'] = ventas_resumen['doc_num'].str.strip()
        ventas_resumen['fec_emis'] = ventas_resumen['fec_emis'].dt.normalize()
        return ventas_resumen.sort_values(by=['fec_emis'], ascending=[True])  # se debe ordenar el df para poder conbinar    
        
    def set_facturas_ventas(self, fecha_ini, fecha_fin):
        return set(self.facturas_ventas_resumen(fecha_ini=fecha_ini, fecha_fin=fecha_fin)['doc_num'])
    
    def facturas_declaradas(self, fecha_ini, fecha_fin):
        set_fact = self.set_facturas_ventas(fecha_ini=fecha_ini, fecha_fin=fecha_fin)
        set_ret_iva = self.retenciones_iva.set_doc_facturas_retenciones_iva()
        set_fact_declaradas =  set_fact & set_ret_iva
        return set_fact_declaradas
        
    def retenciones_iva_clientes_seniat(self):
        return self.retenciones_iva.retenciones_clientes()
    
    def retenciones_iva_clientes_profit(self):
        sql = f"""
                EXEC [RepFormatoCobroRetencion]
                """
        retenciones = get_read_sql(sql, self.conexion)
        retenciones.rename(columns={'fecha': 'fecha_ret'}, inplace=True)
        retenciones['nro_doc'] = retenciones['nro_doc'].str.strip()
        return retenciones.sort_values(by=['fecha_ret'], ascending=[True])[['nro_doc', 'fecha_ret', 'mont_cob']]
        
            
    def facturas_ventas_con_retenciones_iva_profit(self, fecha_ini, fecha_fin):
        data_bcv = self.data_estadisticas_bcv()[['venta_ask2', 'fecha']]
        ventas = self.facturas_ventas_resumen(fecha_ini=fecha_ini, fecha_fin=fecha_fin)
        ventas = merge_asof(ventas, data_bcv, left_on='fec_emis', right_on='fecha', direction="nearest")[['fec_emis', 'doc_num', 'n_control', 'co_cli', 'cli_des', 'venta_ask2', 'total_neto']]
        ventas['total_neto_bs'] = round(ventas['total_neto'] * ventas['venta_ask2'], ndigits=2)
        retenciones = self.retenciones_iva_clientes_profit()
        retenciones[['nro_doc', 'fecha_ret', 'mont_cob']]
        ventas_con_retenciones_iva = merge(ventas, retenciones, how='left', left_on='doc_num', right_on='nro_doc')
        ventas_con_retenciones_iva['mont_cob_bs'] = ventas_con_retenciones_iva['mont_cob'] * round(ventas_con_retenciones_iva['venta_ask2'], ndigits=2)
        ventas_con_retenciones_iva = ventas_con_retenciones_iva[~ventas_con_retenciones_iva['fecha_ret'].isnull()]
        return ventas_con_retenciones_iva[['fec_emis', 
                                           'doc_num', 
                                           'n_control', 
                                           'co_cli', 
                                           'cli_des', 
                                           'venta_ask2', 
                                           'total_neto', 
                                           'total_neto_bs', 
                                           'fecha_ret', 
                                           'mont_cob', 
                                           'mont_cob_bs']]
        
    def cruce_data_retenciones_iva_profit_seniat(self, fecha_ini, fecha_fin):
        retenciones_iva_seniat = self.retenciones_iva_clientes_seniat()
        ventas_con_retenciones_iva = self.facturas_ventas_con_retenciones_iva_profit(fecha_ini=fecha_ini, fecha_fin=fecha_fin)
        cruce_data = merge(retenciones_iva_seniat, ventas_con_retenciones_iva, how='left', left_on='Nro.Documento', right_on='doc_num') 
        return cruce_data[~cruce_data['fec_emis'].isnull()]
            
            
        
if __name__ == '__main__':
    empresa = base_de_datos=getenv('DB_NAME_PROFIT_PANA')
    conexion = ConexionBD(base_de_datos=empresa)
    #print(FacturaVentasConsultas(conexion, empresa=empresa).cruce_data_retenciones_iva_profit_seniat(fecha_ini='20241001', fecha_fin='20241031'))
    FacturaVentasConsultas(conexion, empresa=empresa).cruce_data_retenciones_iva_profit_seniat(fecha_ini='20240101', fecha_fin='20241031').to_excel('cruce_data_retenciones_iva_profit_seniat.xlsx')
    #print(FacturaVentasConsultas(conexion, empresa=empresa).retenciones_iva_clientes_seniat().info())