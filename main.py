import sqlite3
import timeit

databaseName = "villesDeFranceEnUneSeuleTable.db"


# se connecter à la base de données
def connectBase():
    ''' return a connector to the database
    '''
    try:
        conn = sqlite3.connect(databaseName)
        return conn
    except:
        return False


# compte le nombre de ville dans le département 75
def countVillesIn75():
    ''' compte le nombre de ville dans le département 75
    '''
    conn = connectBase()
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM VILLES WHERE department_code = "75"')
    return c.fetchone()[0]


# compte le nombre de ville par département
def countVillesByDepartment():
    ''' compte le nombre de ville par département
    '''
    conn = connectBase()
    c = conn.cursor()
    c.execute('SELECT department_code, COUNT(*) FROM VILLES GROUP BY department_code')
    return c.fetchall()


# compte le nombre de ville par région
def countVillesByRegion():
    ''' compte le nombre de ville par région
    '''
    conn = connectBase()
    c = conn.cursor()
    c.execute('SELECT region_code, COUNT(*) FROM VILLES GROUP BY region_code')
    return c.fetchall()


# lister les villes de Corse
def listVillesCorse():
    ''' lister les villes de Corse
    '''
    conn = connectBase()
    c = conn.cursor()
    c.execute('SELECT name FROM VILLES WHERE region_code = "94"')
    return c.fetchall()


# lister les villes contenant le mot "Seine" trier par département
def listVillesSeine():
    ''' lister les villes contenant le mot "Seine" trier par département
    '''
    conn = connectBase()
    c = conn.cursor()
    c.execute('SELECT name, department_code FROM VILLES WHERE name LIKE "%Seine%" ORDER BY department_code')
    return c.fetchall()


# compter le nombre de villes contenant le mot "Seine" par département
def countVillesSeineByDepartment():
    ''' compter le nombre de villes contenant le mot "Seine" par département
    '''
    conn = connectBase()
    c = conn.cursor()
    c.execute('SELECT department_code, COUNT(*) FROM VILLES WHERE name LIKE "%Seine%" GROUP BY department_code')
    return c.fetchall()


# lance les fonctions
if __name__ == '__main__':
    # compte le nombre de ville dans le département 75
    print('countVillesIn75() =', countVillesIn75())
    # pour chaque département, affiche le nombre de villes
    print('countVillesByDepartment() =')
    for department, count in countVillesByDepartment():
        print('    ', department, count)
    # pour chaque région, affiche le nombre de villes
    print('countVillesByRegion() =')
    for region, count in countVillesByRegion():
        print('    ', region, count)
    # liste les villes de Corse
    print('listVillesCorse() =')
    for ville in listVillesCorse():
        print('    ', ville[0])

    # liste les villes contenant le mot "Seine" trier par département
    print('listVillesSeine() =')
    for ville, department in listVillesSeine():
        print('    ', ville, department)

    # pour chaque département, affiche le nombre de villes contenant le mot "Seine"
    print('countVillesSeineByDepartment() =')
    for department, count in countVillesSeineByDepartment():
        print('    ', department, count)

    # affiche le temps d'exécution de chaque fonction (millisecondes)
    print('countVillesIn75() =', timeit.timeit(countVillesIn75, number=1))
    print('countVillesByDepartment() =', timeit.timeit(countVillesByDepartment, number=1))
    print('countVillesByRegion() =', timeit.timeit(countVillesByRegion, number=1))
    print('listVillesCorse() =', timeit.timeit(listVillesCorse, number=1))
    print('listVillesSeine() =', timeit.timeit(listVillesSeine, number=1))
    print('countVillesSeineByDepartment() =', timeit.timeit(countVillesSeineByDepartment, number=1))
