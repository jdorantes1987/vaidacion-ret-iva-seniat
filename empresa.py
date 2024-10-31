import os
class ClsEmpresa:
    var_st_empresa, var_modulo = '', ''
    
    def __init__(self, modulo_empresa):
        m_empresa = 'DOEL' if modulo_empresa == 'DOEL' else 'PANA'
        self.modulo = modulo_empresa
        ClsEmpresa.var_st_empresa = m_empresa
        ClsEmpresa.var_modulo = modulo_empresa
        
    @staticmethod
    def modulo_seleccionado(): 
        return str(ClsEmpresa.var_st_empresa)
    