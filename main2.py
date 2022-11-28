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


# main
if __name__ == "__main__":
    print("nombre de ville dans le département 75 : ", countVillesIn75())