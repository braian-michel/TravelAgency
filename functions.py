import pandas as pd
import numpy as np
from random import randint, uniform,random, choice
import random

############* Funciones para alterar los datasets originales ##############
def listaAleatorios(n, min_, max_):
    lista = [0]*n # Creo una lista de n elementos (que en este caso son 0s)
    for i in range(n):
          lista[i] = randint(min_, max_)
    return lista


def DatosFicticios (file_name):
    file = pd.ExcelFile(file_name)
    file = file.parse(file.sheet_names[0])
    file = file.iloc[1:]
    n = len(file)
    
    gb = listaAleatorios(n, 20, 700)
    bk = listaAleatorios(n, 1, 4)
    cxl = listaAleatorios(n, 0, 1)
    emi = listaAleatorios(n, 1, 5)
    rate = listaAleatorios(n, 60, 250)
    hab = listaAleatorios(n, 1, 3)
    rn = listaAleatorios(n, 1, 15)
    pax = listaAleatorios(n, 1, 4)
    ant_prom = listaAleatorios(n, 7, 90)
      
    file['GB (USD)'] = gb
    file['Booking'] = bk
    file['Cancelaciones'] = cxl
    file['Emisiones'] = emi
    file['Tarifa (USD)'] = rate
    file['Habitaciones'] = hab
    file['Roomnights'] = rn
    file['#Pasajeros'] = pax
    file['Anticipación Prom.'] = ant_prom
    file['Anticipación Compra'] = file['Anticipación Prom.']
     
    return file


# Para los dataset de destinos
def destinos_aleatorios(df):
    destinos = ['Destino1, Pais1','Destino2, Pais1','Destino3, Pais2','Destino4, Pais2']
    lista = ['']*len(df)
    for i in range(len(lista)):
        lista[i] = random.choice(destinos)
    df['Destino'] = lista
    return df

# Para los dataset de hoteles
def ids(df):
    n = len(df['HotelDespegarId'])
    df['HotelDespegarId'] = listaAleatorios(n, 1000, 1020)
    return df


# Emparejar la suma de las columnas entre datasets del mismo nivel de análisis
def emparejar_datos(col, df_a, df_b):
    x = sum(df_a[col])/sum(df_b[col])
    df_b[col] = df_b[col]*x

# Generar columnas calculadas
def calc_cols(df):
    df['Estadía'] = df['Roomnights']/df['Booking']


########## FUNCIONES HOTELES ###########

# LIMPIAR DATASET
def clean_data_htl(data):
    data.drop(data.columns[14:22],axis=1,inplace=True)  
    data['HotelDespegarId'] = data['HotelDespegarId'].astype(int) 
    return data


#  BASE DE HOTELES
def base_hoteles():
    base = pd.ExcelFile('Base_HTL.xlsx')
    base = base.parse(base.sheet_names[0])
    return base


#UNIR DATA Proveedor+Site con Base
def merge_prov_site(data_prov_site):
    ps = clean_data_htl(data_prov_site)
    base_htl = base_hoteles()
    data = pd.merge(ps, base_htl[['HotelDespegarId','HotelNombre','Destino']],how='left', on='HotelDespegarId')

    # Genera columnas año y semana por separado
    año_sem = data['AÑO_SEMANA'].str.split("-",n=1, expand=True)
    data['Año'] = año_sem[0]
    data['Semana'] = año_sem[1]
    
    # Me quedo solo con estas columnas
    data = data[['AÑO_SEMANA','Año','MES','Semana', 'HotelDespegarId',
       'HotelNombre', 'Destino', 'Site', 'TipoDeContrato', 'GB (USD)',
       'Booking', 'Cancelaciones', 'Emisiones', 'Habitaciones',
       'Roomnights', '#Pasajeros', 'Anticipación Compra', 
        'Estadía', 'Tarifa (USD)']]
    
    data['TipoDeContrato'] = data['TipoDeContrato'].replace('Proveedor','Venta por terceros')
    data['TipoDeContrato'] = data['TipoDeContrato'].replace('Directo','Venta directa')
    data.to_csv('Site+Prov.csv', encoding='utf-8',index=False)
    return data


# UNIR DATA TipoDePago+ ProductoOriginal con Base
def merge_tp_po(data_tppo):
    tppo = clean_data_htl(data_tppo)
    base_htl = base_hoteles()
    data = pd.merge(tppo, base_htl[['HotelDespegarId','Destino','HotelNombre']],how='left', on='HotelDespegarId')

    # Genera columnas año y semana por separado
    año_sem = data['AÑO_SEMANA'].str.split("-",n=1, expand=True)
    data['Año'] = año_sem[0]
    data['Semana'] = año_sem[1]
    
    data = data[['Año', 'MES','Semana', 'HotelDespegarId', 'HotelNombre','Destino', 'TipoDePago', 'ProductoOriginal',
       'GB (USD)', 'Booking', 'Cancelaciones', 'Emisiones', 'Habitaciones',
       'Roomnights', '#Pasajeros', 'Anticipación Compra', 
        'Estadía', 'Tarifa (USD)']]

    data['ProductoOriginal'] = data['ProductoOriginal'].replace('Hoteles','Solo Hotel')
    data['ProductoOriginal'] = data['ProductoOriginal'].replace('Carrito','Vuelo+Hotel')
    data['ProductoOriginal'] = data['ProductoOriginal'].replace('Bundles','Hotel+Excursiones')
    data['ProductoOriginal'] = data['ProductoOriginal'].replace('Escapadas','Hotel+Traslados')
    data['TipoDePago'] = data['TipoDePago'].replace('Precobro de comisión','Pago en efectivo')
    data['TipoDePago'] = data['TipoDePago'].replace('Pago en destino','Pago en efectivo')
    data['TipoDePago'] = data['TipoDePago'].replace('Prepago','Pago con Tarjeta')
    data.to_csv('TP+PO.csv', encoding='utf-8', index=False)    
    return data


#HOTELES FECHA DE ESTADÍA (TipoDeContrato + Site)
def merge_htlci(data_ci):
    htlci = clean_data_htl(data_ci)
    base_htl = base_hoteles()
    data = pd.merge(htlci, base_htl[['HotelDespegarId','Destino','HotelNombre']],how='left', on='HotelDespegarId')

    # Genera columnas año y semana por separado
    año_sem = data['AÑO_SEMANA'].str.split("-",n=1, expand=True)
    data['Año'] = año_sem[0]
    data['Semana'] = año_sem[1]
    
    data = data[['AÑO_SEMANA','Año','MES','Semana', 'HotelDespegarId',
       'HotelNombre', 'Destino', 'Site', 'TipoDeContrato', 'GB (USD)',
       'Booking', 'Cancelaciones', 'Emisiones', 'Habitaciones',
       'Roomnights', '#Pasajeros', 'Anticipación Compra', 
        'Estadía', 'Tarifa (USD)']]
    
    data['TipoDeContrato'] = data['TipoDeContrato'].replace('Proveedor','Venta por terceros')
    data['TipoDeContrato'] = data['TipoDeContrato'].replace('Directo','Venta directa')

    # Creación columna meses en número
    data['Mes_n'] = [0]*len(data)
    meses__ = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic']
    meses = [0]*len(meses__)
    inicio = 1
    for i in range(len(meses)):
        meses[i] = inicio
        inicio = inicio + 1
    for d in range(len(data)):
        for i in range(len(meses)):
            if data.MES[d] == meses__[i]:
                data.Mes_n[d] = meses[i]
    
    data.to_csv('HTLCI.csv', encoding='utf-8', index=False)    
    return data


# HOTELES FECHA DE ESTADÍA (Tipo de pago y producto)
def merge_htlci2(data_ci2):
    htlci2 = clean_data_htl(data_ci2)
    base_htl = base_hoteles()
    data = pd.merge(htlci2, base_htl[['HotelDespegarId','Destino','HotelNombre']],how='left', on='HotelDespegarId')
    
    data = data[['AÑO', 'MES', 'HotelDespegarId', 'HotelNombre','Destino', 
                 'TipoDePago', 'ProductoOriginal','GB (USD)', 'Booking',
                 'Cancelaciones', 'Emisiones', 'Habitaciones','Roomnights', 
                 '#Pasajeros', 'Anticipación Compra', 'Estadía', 'Tarifa (USD)']]

    data['ProductoOriginal'] = data['ProductoOriginal'].replace('Hoteles','Solo Hotel')
    data['ProductoOriginal'] = data['ProductoOriginal'].replace('Carrito','Vuelo+Hotel')
    data['ProductoOriginal'] = data['ProductoOriginal'].replace('Bundles','Hotel+Excursiones')
    data['ProductoOriginal'] = data['ProductoOriginal'].replace('Escapadas','Hotel+Traslados')
    data['TipoDePago'] = data['TipoDePago'].replace('Precobro de comisión','Pago en efectivo')
    data['TipoDePago'] = data['TipoDePago'].replace('Pago en destino','Pago en efectivo')
    data['TipoDePago'] = data['TipoDePago'].replace('Prepago','Pago con Tarjeta')
    
    # Creación columna meses en número
    data['Mes_n'] = [0]*len(data)
    meses__ = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic']
    meses = [0]*len(meses__)
    inicio = 1
    for i in range(len(meses)):
        meses[i] = inicio
        inicio = inicio + 1
    for d in range(len(data)):
        for i in range(len(meses)):
            if data.MES[d] == meses__[i]:
                data.Mes_n[d] = meses[i]
    
    data.to_csv('HTLCI2.csv', encoding='utf-8', index=False)   
    return data


####### FUNCIONES DESTINOS ##########

def clean_data(data):
    #Drop columns
    data.drop(data.columns[14:22],axis=1,inplace=True)  
    data = data.round(2)
    
    # Genero la columna pais a partir de destino
    paises = []
    for i in data['Destino']:
        ini = i.index(', ') + 2 # El pais inicia 2 lugares despues de la coma
        p = i[ini:]
        paises.append(p)      
    data['País'] = paises   
    return data


# LIMPIAR DATA (TipoDePago+PeoductoOriginal)
def clean_tppo(dest_site):
    data = clean_data(dest_site)
    
    # Genera columnas año y semana por separado
    año_sem = data['AÑO_SEMANA'].str.split("-",n=1, expand=True)
    data['Año'] = año_sem[0]
    data['Semana'] = año_sem[1]
    
    data = data[['AÑO_SEMANA','Año', 'Semana','Destino','País','ProductoOriginal', 'TipoDePago','Site',
       'GB (USD)', 'Booking', 'Cancelaciones', 'Emisiones', 'Habitaciones',
       'Roomnights', '#Pasajeros', 'Anticipación Compra', 
        'Estadía', 'Tarifa (USD)']]
    
    data['ProductoOriginal'] = data['ProductoOriginal'].replace('Hoteles','Solo Hotel')
    data['ProductoOriginal'] = data['ProductoOriginal'].replace('Carrito','Vuelo+Hotel')
    data['ProductoOriginal'] = data['ProductoOriginal'].replace('Bundles','Hotel+Excursiones')
    data['ProductoOriginal'] = data['ProductoOriginal'].replace('Escapadas','Hotel+Traslados')
    data['TipoDePago'] = data['TipoDePago'].replace('Precobro de comisión','Pago en efectivo')
    data['TipoDePago'] = data['TipoDePago'].replace('Pago en destino','Pago en efectivo')
    data['TipoDePago'] = data['TipoDePago'].replace('Prepago','Pago con Tarjeta')
    data.to_csv('Destinos.csv', encoding='utf-8', index=False)
    return data
    

# LIMPIAR DATA (Prov+TipoContrato+TipoHotel)
def clean_data_prov(data_dest_prov):
    data = clean_data(data_dest_prov)
    
    # Genera columnas año y semana por separado
    año_sem = data['AÑO_SEMANA'].str.split("-",n=1, expand=True)
    data['Año'] = año_sem[0]
    data['Semana'] = año_sem[1]
    
    data = data[['AÑO_SEMANA','Año', 'Semana','Destino','País','TipoDeContrato', 'Plataforma','TipoHotel',
       'GB (USD)', 'Booking', 'Cancelaciones', 'Emisiones', 'Habitaciones',
       'Roomnights', '#Pasajeros', 'Anticipación Compra', 
        'Estadía', 'Tarifa (USD)']]
    
    data['TipoDeContrato'] = data['TipoDeContrato'].replace('Directo', 'Venta Directa')
    data['TipoDeContrato'] = data['TipoDeContrato'].replace('Proveedor', 'Venta por terceros')
    
    data['Plataforma'] = data['Plataforma'].replace('App','Venta Mobile')
    data['Plataforma'] = data['Plataforma'].replace('Site-Mobile','Venta Mobile')
    data['Plataforma'] = data['Plataforma'].replace('Site-Desktop','Venta Website')
    data.to_csv('Destinos_prov.csv', encoding='utf-8', index=False)
    return data


# REPORTE PICOS DE BÚSQUEDA
def clean_picos(file):
    data = pd.read_csv(file)
    data = destinos_aleatorios(data)   
    data.to_csv('Picos.xlsx', encoding='utf-8', index=False)
    return data


# VENTAS POR DESTINO - FEHCA DE CHECK IN (PREVIUS WEEK)
def clean_destci(data_dest_ci):
    data = clean_data(data_dest_ci)
    fecha = data['FECHA'].astype(str)
    AÑO = fecha.str.split("-",n=2, expand=True)
    data['Año'] = AÑO[0]
    data['Mes'] = AÑO[1]

    data = data[['Año','Mes', 'FECHA','Destino','TipoDeContrato', 'TipoDePago','Site',
       'GB (USD)', 'Booking', 'Cancelaciones', 'Emisiones', 'Habitaciones',
       'Roomnights', '#Pasajeros', 'Anticipación Compra', 
        'Estadía', 'Tarifa (USD)']]
    data['TipoDeContrato'] = data['TipoDeContrato'].replace('Directo', 'Venta Directa')
    data['TipoDeContrato'] = data['TipoDeContrato'].replace('Proveedor', 'Venta por terceros')
    data['TipoDePago'] = data['TipoDePago'].replace('Precobro de comisión','Pago en efectivo')
    data['TipoDePago'] = data['TipoDePago'].replace('Pago en destino','Pago en efectivo')
    data['TipoDePago'] = data['TipoDePago'].replace('Prepago','Pago con Tarjeta')
    data.to_csv('Dest_CI.csv', encoding='utf-8', index=False)   
    return data