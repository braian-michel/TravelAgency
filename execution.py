import pandas as pd
import numpy as np
from random import randint, uniform,random

from functions import listaAleatorios
from functions import DatosFicticios
from functions import destinos_aleatorios
from functions import ids
from functions import emparejar_datos
from functions import calc_cols
from functions import clean_data_htl
from functions import base_hoteles
from functions import merge_prov_site
from functions import merge_tp_po
from functions import merge_htlci
from functions import merge_htlci2
from functions import clean_data
from functions import clean_tppo
from functions import clean_data_prov
from functions import clean_picos
from functions import clean_destci


# Datos aleatorios generados a partir de los dataset descargados
# ULTIMA VERSIÓN (MAYO 2021 - menos código)
dest_files = ['DestSite.xlsx', 'DestProv.xlsx', 'DestCI.xlsx']
htl_files = ['HTLProv.xlsx', 'HTLTPPO.xlsx', 'HTLCI.xlsx', 'HTLCI2.xlsx']
df_list = []

for i in range(len(dest_files)):
    exec("dest%d = DatosFicticios(dest_files[i])" %i)
    exec("dest%d = destinos_aleatorios(dest%d)" % (i,i))
    exec("df_list.append(dest%d)" % i)
    

for i in range(len(htl_files)):
    exec("htl%d = DatosFicticios(htl_files[i])" % i )
    exec("htl%d = ids(htl%d)" % (i,i))
    exec("df_list.append(htl%d)" % i)


# Emparejar los datos de Bk, RN, Ant, ADR, HAB, PAX
col_list = ['Booking','Roomnights','Emisiones','Anticipación Compra', 'Tarifa (USD)',
            'Cancelaciones', 'Habitaciones', '#Pasajeros']

# Se busca emparejar los totales, las sumas de los valores para que coincidan entre los datasets
for i in col_list:
    emparejar_datos(i,dest0, dest1) # DestSite y Prov
    emparejar_datos(i,htl0, htl1) # HTLProv y TPPO
    emparejar_datos(i,htl2, htl3) # HTLCI y HTLCI2
    
for i in df_list:
    calc_cols(i)

clean_picos('Picos BR.csv')

# Destinos - fecha reserva
clean_tppo(dest0)
clean_data_prov(dest1)

# Hoteles - fecha reserva
merge_prov_site(htl0)
merge_tp_po(htl1)

# Destinos - fecha check in
clean_destci(dest2)

# Hoteles - fecha check in
merge_htlci(htl2)
merge_htlci2(htl3)