from __future__ import print_function
import pickle
import os.path
import csv
import time
import argparse
import sys
import mysql.connector
from mysql.connector import errorcode
from dateutil.parser import parse
import pytz
from pathlib import Path
from apiclient import errors
from apiclient import http
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from apiclient.http import MediaFileUpload


def variables():
    with open("variables.txt") as f:
        lineas = f.read().splitlines() 
    usuarioBD = (lineas[1])
    passw = (lineas[2])
    BD = (lineas[3])
    rutaC = (lineas[4])
    usuario = usuarioBD.replace('usuario = ', '')
    contrasena = passw.replace('contrasena = ', '')
    basedeDatos = BD.replace('basedatos = ', '')
    rutaCred = rutaC.replace('ruta_credencial = ', '')
    return usuario, contrasena, basedeDatos, rutaCred

def Credenciales(rutaCred):
#Implementacion de la API de Google Drive
#Entrega las credenciales para utilizar
     creds = None
     if os.path.exists('token.pickle'):
         with open('token.pickle', 'rb') as token:
             creds = pickle.load(token)
     if not creds or not creds.valid:
         if creds and creds.expired and creds.refresh_token:
             creds.refresh(Request())
         else:
             flow = InstalledAppFlow.from_client_secrets_file(
                 rutaCred+'credentials.json', ['https://www.googleapis.com/auth/drive'])
             creds = flow.run_local_server(port=0)
         with open('token.pickle', 'wb') as token:
             pickle.dump(creds, token)
     return creds
    
def conversionExtension(extension):
    extOf = []
    extensionOficial = { 
                'all': "or mimeType != 'application/vnd.google-apps.folder'",
                'pdf': "or mimeType contains 'application/pdf'", 
                'google-xls': "or mimeType='application/vnd.google-apps.spreadsheet'", 
                'google-doc': "or mimeType='application/vnd.google-apps.document'",
                'google-ppt': "or mimeType='application/vnd.google-apps.presentation'", 
                'txt': "or mimeType='text/plain'",
                'csv': "or mimeType='text/csv'",
                'doc': "or mimeType contains 'application/msword'",
                'docx': "or mimeType='application/vnd.openxmlformats-officedocument.wordprocessingml.document'",
                'xls': "or mimeType='application/vnd.ms-excel'"     
    }
    return [extensionOficial[x] for x in extension]

    
def realizarBusqueda(servicio, busqueda, extensionOficial):
#Realiza la busqueda con parametros
#Retorna la lista de archivos
    nombreArchivo = busqueda
    extensionsinFormato = ', '.join(map(str, extensionOficial))
    extensionSinComa = str(extensionsinFormato).replace( ','  , '' )
    extension = (extensionSinComa.partition(' ')[2]) 
    resultado = servicio.files().list(q="name contains '"+nombreArchivo+"' and ("+extension+")",
                            includeItemsFromAllDrives='true',
                            supportsAllDrives='true',
                            corpora='allDrives',
                            fields="nextPageToken, files(name, createdTime, modifiedTime, owners, parents, webViewLink)").execute()
    archivos = resultado.get('files', [])
    return archivos

def obtenerRutas(servicio, archivo):
    parent=archivo.get('parents')
    tree = []
    ruta = ""
    while True:
        folder = servicio.files().get( fileId=parent[0], fields='id, name, parents').execute()
        parent = folder.get('parents')
        if parent is None:
            break
        tree.append({'id': parent[0], 'name': folder.get('name')})
        ruta = folder.get('name')+"/"+ruta
    ruta = ruta+""+archivo.get('name')   
    linkFolder = "https://drive.google.com/drive/folders/"+archivo.get('parents')[0]
    linkFile = archivo.get('webViewLink')
    return ruta, linkFolder, linkFile
    
    
def obtenerDatosArchivos(archivos, servicio):
#Funcion que procesa los datos
#Crea una lista utilizando los archivos y sus autores
    TotalArchivos = []
    ListadeArchivos = []
    if not archivos:
        return ''
    for archivo in archivos:  
        autores = archivo.get('owners',[])
        rutaArchivo, enlaceCarpeta, enlaceArchivo = obtenerRutas(servicio, archivo)
        for autor in autores:
            fechaC = archivo.get('createdTime')
            fechaM = archivo.get('modifiedTime')
            fechaCrea = parse(fechaC)
            fechaMod = parse(fechaM)
            fechacSinZona = fechaCrea.replace(tzinfo=None)
            fechamSinZona = fechaMod.replace(tzinfo=None)
            fechaCreacion = fechacSinZona.strftime("%d-%m-%Y %H:%M:%S")
            fechaModificacion = fechamSinZona.strftime("%d-%m-%Y %H:%M:%S")
            TodosArchivos = (archivo['name']), str(fechaCreacion), str(fechaModificacion), (autor['displayName']), rutaArchivo, enlaceCarpeta, enlaceArchivo
            ListadeArchivos.append(TodosArchivos)
    return ListadeArchivos    

            
def imprimirArchivosxPantalla(ListadeArchivos):
#Imprime la lista de archivos encontrados
    for TodosArchivos in ListadeArchivos:
        print(TodosArchivos)

def guardarArchivo(ListadeArchivos, depto):
#Guarda en un archivo CSV los archivos encontrados
    if (os.path.isfile(depto+'.csv'))== True: 
        with open(depto+'.csv', 'a', newline='') as file:
            a = csv.writer(file, delimiter = ',')
            a.writerows(ListadeArchivos)
    else:
        with open(depto+'.csv', 'w', newline='') as file:
            nombres=['Nombre','Fecha Creacion','Fecha Modificacion','Autor','ruta Archivo', 'enlace Carpeta', 'enlace Archivo']
            a = csv.writer(file)
            a.writerow(nombres)
            a.writerows(ListadeArchivos)

def buscarCarpeta(depto, servicio):
#Busqueda carpeta para crearla o subir archivo
    resultado = servicio.files().list(q="mimeType = 'application/vnd.google-apps.folder' and name='"+depto+"'and trashed=false",
                            includeItemsFromAllDrives='true',
                            supportsAllDrives='true',
                            corpora='allDrives',
                            fields="files(id)").execute()
    carpetas = resultado.get('files', [])
    return carpetas
    
def crearCarpeta(carpetas, servicio, depto):
#Creacion de carpeta y se pasa el ID para subir archivo
    if len(carpetas) == 0:
        datosArchivo = {
                        'name': depto,
                        'mimeType': 'application/vnd.google-apps.folder'
                        }
        archivo = servicio.files().create(body=datosArchivo,
                                    fields='id').execute()
        idCarpeta = archivo.get('id')
        return idCarpeta
    else:
        idCarpeta = carpetas[0]['id']
        return idCarpeta
        
def busquedaArchivoenDrive(servicio, depto):
#Se busca si el archivo existe en el Drive
    resultado = servicio.files().list(q="mimeType != 'application/vnd.google-apps.folder' and name contains '"+depto+"'and trashed=false",
                            includeItemsFromAllDrives='true',
                            supportsAllDrives='true',
                            corpora='allDrives',
                            fields="nextPageToken, files(id)").execute()
    archivoEncontrado = resultado.get('files', [])
    return archivoEncontrado
        
def subirArchivoaCarpeta(servicio, depto, idCarpeta, archivoEncontrado):
#Se sube archivo o si existe, se actualiza en el Drive
    directorio = Path(__file__).parent
    if archivoEncontrado == []:
        datosArchivo = {
                        'name': depto+'.csv',
                        'parents' : [idCarpeta]
                        }   
        media = MediaFileUpload(str(directorio)+'/'+depto+'.csv',
                            mimetype='text/csv',
                            resumable=True)
        archivo = servicio.files().create(body=datosArchivo,
                                        media_body=media,
                                        fields='id').execute()
        print ("Archivo subido a Drive")
    else:
        idArchivo = archivoEncontrado[0]['id']
        datosArchivo = {
                        'name': depto+'.csv'
                        }   
        media = MediaFileUpload(str(directorio)+'/'+depto+'.csv',
                            mimetype='text/csv',
                            resumable=True)
        archivo = servicio.files().update(fileId=idArchivo,
                                        body=datosArchivo,
                                        addParents=idCarpeta,
                                        media_body=media,
                                        fields='id').execute()
        print ("Archivo actualizado en Drive")

def conexionBd(usuario, contrasena, basedeDatos):
    cnx = mysql.connector.connect(user=usuario, password = contrasena, database = basedeDatos)
    return cnx

def creacionBd(cnx):
    cursor = cnx.cursor()
    nombreDB = 'segcotizacion'
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(nombreDB))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))

    try:
        cursor.execute("USE {}".format(nombreDB))
    except mysql.connector.Error as err:
        print("Database {} does not exists.".format(nombreDB))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor)
            print("Database {} created successfully.".format(nombreDB))
            cnx.database = database
        else:
            print(err)
            exit(1)
    return cursor
    
def creacionTablas():
    tablas = {}
    tablas['busqueda'] = (
    "CREATE TABLE `busqueda` ("
    "  `id_Busqueda` int(11) NOT NULL AUTO_INCREMENT," 
    "  `palabra_Clave` varchar(255) NOT NULL,"
    "  `departamento` varchar(255) NOT NULL,"
    "  `responsable_Busqueda` varchar(255) NOT NULL,"
    "   PRIMARY KEY (`id_Busqueda`)"
    ") ENGINE=InnoDB")

    tablas['resultado'] = (
        "CREATE TABLE `resultado` ("
        "  `id_Resultado` int(11) NOT NULL AUTO_INCREMENT,"
        "  `enlace_Carpeta` varchar(255) NOT NULL,"
        "  `hora_Busqueda` DATETIME NOT NULL, "
        "  `id_Busqueda` int(11) NOT NULL," 
        "   PRIMARY KEY (`id_Resultado`),"
        "   CONSTRAINT `id_Busqueda_Fk` FOREIGN KEY (`id_Busqueda`)"
        "   REFERENCES `busqueda` (`id_Busqueda`)"
        ") ENGINE=InnoDB")

    tablas['archivos'] = (
        "CREATE TABLE `archivos` ("
        "  `id_Archivo` int(11) NOT NULL AUTO_INCREMENT,"
        "  `nombre_Archivo` varchar (255) NULL,"
        "  `fecha_Creacion` DATETIME NOT NULL,"
        "  `fecha_Modificacion` DATETIME NOT NULL,"
        "  `autor` varchar(255) NOT NULL,"
        "  `ruta_Archivo` varchar(255) NOT NULL,"
        "  `enlace_Carpeta` varchar(255) NOT NULL,"
        "  `enlace_Archivo` varchar(255) NOT NULL,"
        "  `id_Resultado` int(11) NOT NULL,"
        "   PRIMARY KEY (`id_Archivo`),"
        "   CONSTRAINT `id_Resultado_Fk` FOREIGN KEY (`id_Resultado`)"
        "   REFERENCES `resultado` (`id_Resultado`)"
        ") ENGINE=InnoDB")
    return(tablas)

def encargadoBusqueda(servicio):
    resp =  servicio.about().get(fields='user').execute()
    for email in resp.values():
        emailResponsableBusqueda = email['emailAddress']
    return emailResponsableBusqueda


def creacionTablaenBD(tablas, cursor):
    for tablaBD in tablas:
        descripcionTabla = tablas[tablaBD]
        try:
            print("Creating table {}: ".format(tablaBD), end='')
            cursor.execute(descripcionTabla)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

def insertarenBD(ListadeArchivos, cursor, cnx, bus, depto, idCarpeta, emailResponsableBusqueda):
    linkCarpetaArchivo = "https://drive.google.com/drive/folders/"+idCarpeta
    horaBusqueda = time.strftime('%Y-%m-%d %H:%M:%S')
    archivosEncontrados = ("INSERT INTO archivos "
               "(Nombre_archivo, Fecha_creacion, Fecha_modificacion, Autor, Ruta_archivo, Enlace_carpeta, Enlace_archivo, id_Resultado) "
               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
                  
    nuevaBusqueda = ("INSERT INTO busqueda "
                    "(Palabra_clave, Departamento, Responsable_busqueda) "
                    "VALUES (%s, %s, %s)")
   
    datosBusqueda = (bus, depto, emailResponsableBusqueda)
    cursor.execute(nuevaBusqueda, datosBusqueda)
    
    fkidBusqueda = cursor.lastrowid
    
    
    resultadoBusqueda = ("INSERT INTO resultado "
                      "(Enlace_carpeta, Hora_busqueda, id_Busqueda)"
                      "VALUES (%s, %s, %s)")
                      
    valoresResultadoBusqueda = (linkCarpetaArchivo, horaBusqueda, fkidBusqueda)
    cursor.execute(resultadoBusqueda, valoresResultadoBusqueda)  
    
    fkIdResultado = cursor.lastrowid
    
    for insertar in ListadeArchivos:
        fechaCreacion = (insertar[1])
        fechaModificacion = (insertar[2])
        fechaCreacionparaBD = parse(fechaCreacion)
        fechaModificacionparaBD = parse(fechaModificacion)
        fechaSinZonaHoraria = fechaCreacionparaBD.replace(tzinfo=None)
        fechaModSinZonaHoraria = fechaModificacionparaBD.replace(tzinfo = None)
        fechaCreacionFormatoBD = fechaSinZonaHoraria.strftime('%Y-%m-%d %H:%M:%S')
        fechaModificacionFormatoBD = fechaModSinZonaHoraria.strftime('%Y-%m-%d %H:%M:%S')
        listaParaInsertar =  list(insertar)
        listaParaInsertar[1] =  fechaCreacionFormatoBD   
        listaParaInsertar[2] = fechaModificacionFormatoBD
        listaParaInsertar.insert(7, fkIdResultado)
        insertar = tuple(listaParaInsertar)
        cursor.execute(archivosEncontrados, insertar)                  

    cnx.commit()
    cursor.close()
    cnx.close()
    
    
def main():
#Comienza el programa, pide los parametros y llama a las funciones   
    usuario, contrasena, basedeDatos, rutaCred = variables()
    creds = Credenciales(rutaCred)
    servicio = build('drive', 'v3', credentials=creds)
    parser = argparse.ArgumentParser(description='Buscar archivos')
    parser.add_argument('-b', nargs='?', dest='busqueda', type=str, required=True,
                help='ingresar nombre de archivo')
    parser.add_argument('-dep', nargs='?', dest='dep', type=str, required=True,
                help="""ingresar departamento""")
    parser.add_argument('-e', type=str, nargs='?', dest='extension', action='append', const = 'all',
                help="""ingresar 1 o N extensiones""")      
    args = parser.parse_args()
    bus,ext,depto = validarDatos(args)
    
    extensionFormatoDrive = conversionExtension(ext)
    busquedaArchivos = realizarBusqueda(servicio, bus, extensionFormatoDrive)
    TraerDatos = obtenerDatosArchivos(busquedaArchivos, servicio)
    if TraerDatos == '':
        pass
    else:
        #imprimirArchivosxPantalla(TraerDatos)
        guardarArchivo(TraerDatos, depto)           
        buscarCarpetaDrive = buscarCarpeta(depto, servicio)
        creacionCarpetaDrive = crearCarpeta(buscarCarpetaDrive, servicio, depto)
        busquedaArchivosActualizar = busquedaArchivoenDrive(servicio, depto)
        subirArchivoaCarpeta(servicio, depto, creacionCarpetaDrive, busquedaArchivosActualizar)
        conexion = conexionBd(usuario, contrasena, basedeDatos)
        cursor = creacionBd(conexion)        
        creacionTablas()
        responsableBusqueda = encargadoBusqueda(servicio)
        tablas = creacionTablas()
        creacionTablaenBD(tablas, cursor)
        insertarenBD(TraerDatos, cursor, conexion, bus, depto, creacionCarpetaDrive, responsableBusqueda)
    
def validarDatos(args):
    bus = args.busqueda
    dep = args.dep      
    extr = args.extension
    year = time.strftime("%Y")
    depto = dep+'-'+year
    if extr is None:
        ext = ['all']
    else:
        ext = extr
    return bus, ext, depto
    
if __name__ == '__main__':
    main()
