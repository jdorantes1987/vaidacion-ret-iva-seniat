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
        data_retenciones = self.retenciones_iva.retenciones_clientes()
        return data_retenciones.groupby(['Rif Agente Retención', 
                                                     'Agente Retención', 
                                                     'Fecha Documento',
                                                     'Nro.Documento',
                                                     'Nro. Control Documento']).agg(
                                                                                    {'Monto del Documento':'sum',
                                                                                     'Monto Retenido':'sum',
                                                                                     'Monto exento':'sum'}).reset_index()
    
    def retenciones_iva_clientes_profit(self):
        sql = f"""
                EXEC [RepFormatoCobroRetencion]
                """
        retenciones = get_read_sql(sql, self.conexion)
        retenciones.rename(columns={'fecha': 'fecha_ret'}, inplace=True)
        retenciones['nro_doc'] = retenciones['nro_doc'].str.strip()
        return retenciones.sort_values(by=['fecha_ret'], ascending=[True])[['cob_num', 'nro_doc', 'fecha_ret', 'mont_cob']]
        
            
    def facturas_ventas_con_retenciones_iva_profit(self, fecha_ini, fecha_fin):
        data_bcv = self.data_estadisticas_bcv()[['venta_ask2', 'fecha']]
        ventas = self.facturas_ventas_resumen(fecha_ini=fecha_ini, fecha_fin=fecha_fin)
        ventas = merge_asof(ventas, data_bcv, left_on='fec_emis', right_on='fecha', direction="nearest")[['fec_emis', 'doc_num', 'n_control', 'co_cli', 'cli_des', 'venta_ask2', 'total_neto']]
        ventas['total_neto_bs'] = round(ventas['total_neto'] * round(ventas['venta_ask2'], ndigits=2) , ndigits=2)
        retenciones = self.retenciones_iva_clientes_profit()
        ventas_con_retenciones_iva = merge(ventas, retenciones, how='left', left_on='doc_num', right_on='nro_doc')
        ventas_con_retenciones_iva['mont_cob_bs'] = round(ventas_con_retenciones_iva['mont_cob'] * round(ventas_con_retenciones_iva['venta_ask2'], ndigits=2), ndigits=2)
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
                                           'cob_num',
                                           'mont_cob', 
                                           'mont_cob_bs']]
        
    def cruce_data_retenciones_iva_profit_seniat(self, fecha_ini, fecha_fin):
        retenciones_iva_seniat = self.retenciones_iva_clientes_seniat()
        ventas_con_retenciones_iva = self.facturas_ventas_con_retenciones_iva_profit(fecha_ini=fecha_ini, fecha_fin=fecha_fin)
        cruce_data = merge(retenciones_iva_seniat, ventas_con_retenciones_iva, how='left', left_on='Nro.Documento', right_on='doc_num') 
        return cruce_data
    
    def diferencias_cruce(self, fecha_ini, fecha_fin):
        cruce_data = self.cruce_data_retenciones_iva_profit_seniat(fecha_ini=fecha_ini, 
                                                                   fecha_fin=fecha_fin)[['Rif Agente Retención', 
                                                                                         'co_cli',
                                                                                         'Agente Retención',
                                                                                         'fec_emis',
                                                                                         'doc_num',
                                                                                         'Nro. Control Documento',
                                                                                         'n_control',
                                                                                         'cob_num',
                                                                                         'venta_ask2',
                                                                                         'Monto del Documento',
                                                                                         'Monto Retenido',
                                                                                         'total_neto_bs',
                                                                                         'mont_cob_bs']]                                                           
        
        cruce_data = cruce_data[~cruce_data['fec_emis'].isnull()]
        cruce_data['fec_emis'] = cruce_data['fec_emis'].dt.strftime('%d-%m-%Y')
        cruce_data['dif_monto_ret'] = round(cruce_data['Monto Retenido'] - cruce_data['mont_cob_bs'], ndigits=2)
        cruce_data['monto_ret_seniat_vs_profit'] = cruce_data.apply(lambda x: '(+) de más'  if (x['Monto Retenido'] - x['mont_cob_bs']) > 0 else '(-) de menos', axis=1)
        cruce_data['n_ctrol_seniat_vs_profit'] = cruce_data.apply(lambda x: ''  if (x['Nro. Control Documento'] == x['n_control']) else 'verificar n.control', axis=1)
        cruce_data.rename(columns={'venta_ask2':'tasa',
                                   'Monto Retenido':'ret_seniat',
                                   'mont_cob_bs':'ret_profit',
                                   'Nro. Control Documento':'nro_ctrol_declar'}, inplace=True)
        
        return cruce_data[['co_cli',
                           'Rif Agente Retención', 
                           'Agente Retención',
                           'fec_emis',
                           'doc_num',
                           'n_control',
                           'nro_ctrol_declar',
                           'n_ctrol_seniat_vs_profit',
                           'cob_num',
                           'ret_seniat',
                           'ret_profit',
                           'dif_monto_ret',
                           'monto_ret_seniat_vs_profit']].reset_index(drop=True)
    
    def retenciones_declaradas_sin_cruzar_en_profit(self, fecha_ini, fecha_fin):
        cruce_data = self.cruce_data_retenciones_iva_profit_seniat(fecha_ini=fecha_ini, 
                                                                   fecha_fin=fecha_fin)[['Rif Agente Retención', 
                                                                                         'Agente Retención',
                                                                                         'Nro.Documento',                
                                                                                         'Nro. Control Documento',
                                                                                         'Fecha Documento',
                                                                                         'fec_emis',
                                                                                         'Monto del Documento',
                                                                                         'Monto Retenido'
                                                                                         ]] 
        if len(cruce_data) > 0 :                                                                  
            cruce_data['Fecha Documento'] = cruce_data['Fecha Documento'].dt.strftime('%d-%m-%Y')                                          
        return cruce_data[cruce_data['fec_emis'].isnull()]
            
        
if __name__ == '__main__':
    empresa = base_de_datos=getenv('DB_NAME_PROFIT_PANA')
    conexion = ConexionBD(base_de_datos=empresa)
    FacturaVentasConsultas(conexion, empresa=empresa).diferencias_cruce(fecha_ini='20240101', 
                                                                        fecha_fin='20241031').to_excel('diferencias_cruce.xlsx')
    
    FacturaVentasConsultas(conexion, empresa=empresa).retenciones_declaradas_sin_cruzar_en_profit(fecha_ini='20240101', 
                                                                                                  fecha_fin='20241031').to_excel('retenciones_declaradas_sin_cruzar_en_profit.xlsx')