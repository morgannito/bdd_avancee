import sqlite3
import timeit

databaseName = "tableopti.db"


# se connecter à la base de données
def connectBase():
    ''' return a connector to the database
    '''
    try:
        conn = sqlite3.connect(databaseName)
        return conn
    except:
        return False


# fait un select sur la table departements avec le code du département
def getDepartementIdByCode(code):
    ''' fait un select sur la table departements avec le code du département
    '''
    conn = connectBase()
    c = conn.cursor()
    c.execute('SELECT id FROM DEPARTMENTS WHERE code = ?', (code,))
    return c.fetchone()[0]

# fait un select sur la table regions avec le code de la région
def getRegionIdByCode(code):
    ''' fait un select sur la table regions avec le code de la région
    '''
    conn = connectBase()
    c = conn.cursor()
    c.execute('SELECT id FROM REGIONS WHERE code = ?', (code,))
    return c.fetchone()[0]

# count the number of cities in the department 75
def countVillesIn75():
    ''' count the number of cities in the department 75
    '''
    conn = connectBase()
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM VILLES WHERE department_id = ?', (getDepartementIdByCode("75"),))
    return c.fetchone()[0]


# compte le nombre de ville par département
def countVillesByDepartment():
    ''' compte le nombre de ville par département
    '''
    conn = connectBase()
    c = conn.cursor()
    c.execute('SELECT department_id, COUNT(*) FROM VILLES GROUP BY department_id')
    return c.fetchall()

# compte le nombre de ville par région
def countVillesByRegion():
    ''' compte le nombre de ville par région
    '''
    conn = connectBase()
    c = conn.cursor()
    c.execute('SELECT region_id, COUNT(*) FROM villes_regions GROUP BY region_id')
    return c.fetchall()

# lister les villes de Corse
def listVillesCorse():
    ''' lister les villes de Corse
    '''
    conn = connectBase()
    c = conn.cursor()
    c.execute('SELECT ville_id FROM villes_regions WHERE region_id = ?', (getRegionIdByCode("94"),))
    return c.fetchall()

#
# print(countVillesIn75())
#
# for i in countVillesByDepartment():
#     print(i)

# for i in countVillesByRegion():
#     print(i)

# for i in listVillesCorse():
#     print(i)

# lister les villes contenant le mot "Seine" trier par département
def listVillesSeine():
    ''' lister les villes contenant le mot "Seine" trier par département
    '''
    conn = connectBase()
    c = conn.cursor()
    c.execute('SELECT name, department_id FROM VILLES WHERE name LIKE "%Seine%" ORDER BY department_id')
    return c.fetchall()

# for i in listVillesSeine():
#     print(i)

# compte les villes contenant le mot "Seine" trier par departement
def countVillesSeine():
    ''' compte les villes contenant le mot "Seine" trier par departement
    '''
    conn = connectBase()
    c = conn.cursor()
    c.execute('SELECT department_id, COUNT(*) FROM VILLES WHERE name LIKE "%Seine%" GROUP BY department_id')
    return c.fetchall()

# for i in countVillesSeine():
#     print(i)


if __name__ == '__main__':
    print(timeit.timeit("countVillesIn75()", setup="from __main__ import countVillesIn75", number=1))
    print(timeit.timeit("countVillesByDepartment()", setup="from __main__ import countVillesByDepartment", number=1))
    print(timeit.timeit("countVillesByRegion()", setup="from __main__ import countVillesByRegion", number=1))
    print(timeit.timeit("listVillesCorse()", setup="from __main__ import listVillesCorse", number=1))
    print(timeit.timeit("listVillesSeine()", setup="from __main__ import listVillesSeine", number=1))
    print(timeit.timeit("countVillesSeine()", setup="from __main__ import countVillesSeine", number=1))