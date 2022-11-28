#-*- coding: utf-8 -*-
''' Exemple pour IMERIR - GDLE/Gestion des Données à Large Echelle
    © Pascal Delcombel / Agil'Spaces
'''
import sqlite3
from sqlite3 import Error
from pathlib import Path
from time import time

databaseName = "villesDeFranceEnUneSeuleTable2.db"
fields       = { 'VILLES': [
                        ['id'                  ,'int'],
                        ['insee_code'          ,'text'],
                        ['zip_code'            ,'text'],
                        ['name'                ,'text'],
                        ['slug'                ,'text'],
                        ['gps_lat'             ,'real'],
                        ['gps_lng'             ,'real'],
                        ['department_code'     ,'text'],
                        ['department_name'     ,'text'],
                        ['department_slug'     ,'text'],
                        ['region_code'         ,'text'],
                        ['region_name'         ,'text'],
                        ['region_slug'         ,'text']] }

def connectBase():
    ''' return a connector to the database
    '''
    try:
        conn = sqlite3.connect(databaseName)
        return conn
    except:
        return False
    
def baseExists():
    ''' check if a database exists with the appropriate name
            . we check if the file exists
            . AND we check if we can open it as a sqlite3 database
    '''
    file = Path(databaseName)
    if file.exists () and connectBase():
        return True
    createBase()
    return False

def createBase():
    ''' create the database with the 3 tables
    '''

    #create first the database
    try:
        conn = sqlite3.connect(databaseName)
    except Error as e:
        quit

    c = conn.cursor()
    for table,fieldList in  fields.items():
        sSQL = 'CREATE TABLE {} ( '.format(table)
        for field,fieldtype in fieldList:
            sSQL += ' ' + field + ' ' + fieldtype + ', '
        sSQL = sSQL[:-2] + ')'
        print(sSQL)
        c.execute(sSQL)
    conn.commit()
    conn.close()

def loadFile(fileName, table):
    with open(fileName,'r') as fn:
        skip = True
        while True:
            line = fn.readline()
            if not line:
                break
            record = line.split(',')
            if skip:
                skip = False
            else:
                myDict = {'table': table}
                for (field,fieldtype),value in zip(fields[table],record):
                    myDict[field] = value
                addRecord(**myDict)
    

def addRecord(**dicty):
    ''' add a record in the database
    '''
    table = dicty['table']
    pid   = dicty.get('id',None)
    if not table in fields:         return 'unknown table'
    if pid == None:                 return 'unknown id'
    with connectBase() as conn:   
        c = conn.cursor()
        rSQL = '''DELETE FROM {} WHERE id = '{}' ''' 
        c.execute(rSQL.format(table,pid))
        rSQL = '''INSERT INTO {} ('''.format(table)
        sSQL = ''' VALUES ('''
        for field,fieldtype in fields[table]:
            rSQL +=  field + ', '
            sSQL += '"' + dicty.get(field,'').replace('"','') + '", '
        rSQL = rSQL[:-2] + ') ' + sSQL[:-2] + ") ;"
        c.execute(rSQL)
        conn.commit()
    return True


def __test__():
    ''' unit tests of this module functions
    '''
    print("test de l'existence de la base")
    if not baseExists():
        start = time()
        loadFile('villesEnUneTable.csv','VILLES')
        print("..je viens de créer et charger la base")
        end = time()
        elapsed = int(end - start)
        print("Temps d'exécution : {} ms".format(elapsed))
    else:
        print("..", "la base existe déjà !")

    print("Listons les trucs dedans")
    with connectBase() as conn:   
        c = conn.cursor()
        print("")
        print("Liste")
        c.execute("select * from VILLES ;")
        for record in c.fetchall():
            print(record)
        print("")
    
        
if __name__ == '__main__':
    __test__()
