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

# normalise la base de données en utilisant la norme 3NF
def normaliseBase():
    ''' normalise la base de données en utilisant la norme 3NF
    '''
    # créer une table villes2 avec les champs id, insee_code, zip_code, name, slug, gps_lat, gps_lng et department_id
    conn = connectBase()
    c = conn.cursor()
    c.execute('CREATE TABLE villes2 (insee_code INTEGER PRIMARY KEY, zip_code int, name TEXT, slug TEXT, gps_lat TEXT, gps_lng TEXT, department_id INTEGER)')
    conn.commit()

    # créer une table departments avec les champs id, code, name, slug et region_id
    c.execute('CREATE TABLE departments (code INTEGER PRIMARY KEY, name TEXT, slug TEXT, region_id INTEGER)')
    conn.commit()

    # créer une table regions avec les champs id, code, name, slug
    c.execute('CREATE TABLE regions (code INTEGER PRIMARY KEY, name TEXT, slug TEXT)')
    conn.commit()

    # créer une table villes_departments avec les champs id, ville_id et department_id
    c.execute('CREATE TABLE villes_departments (id INTEGER PRIMARY KEY, ville_id INTEGER, department_id INTEGER)')
    conn.commit()

    # créer une table villes_regions avec les champs id, ville_id et region_id
    c.execute('CREATE TABLE villes_regions (id INTEGER PRIMARY KEY, ville_id INTEGER, region_id INTEGER)')
    conn.commit()

    # créer une table villes_departments_regions avec les champs id, ville_id, department_id et region_id
    c.execute('CREATE TABLE villes_departments_regions (id INTEGER PRIMARY KEY, ville_id INTEGER, department_id INTEGER, region_id INTEGER)')
    conn.commit()

    # remplir les tables
    c.execute('SELECT * FROM villes')
    villes = c.fetchall()

    for ville in villes:
        # remplir la table villes2
        c.execute('INSERT INTO villes2 (insee_code, zip_code, name, slug, gps_lat, gps_lng, department_id) VALUES (?, ?, ?, ?, ?, ?, ?)', (ville[1], ville[2], ville[3], ville[4], ville[5], ville[6], ville[7]))
        conn.commit()
        # remplir la table departments
        c.execute('INSERT INTO departments (code, name, slug, region_id) VALUES (?, ?, ?, ?)', (ville[7], ville[8], ville[9], ville[10]))
        conn.commit()
        # remplir la table regions
        c.execute('INSERT INTO regions (code, name, slug) VALUES (?, ?, ?)', (ville[10], ville[11], ville[12]))
        conn.commit()
        # remplir la table villes_departments
        c.execute('INSERT INTO villes_departments (ville_id, department_id) VALUES (?, ?)', (ville[0], ville[7]))
        conn.commit()
        # remplir la table villes_regions
        c.execute('INSERT INTO villes_regions (ville_id, region_id) VALUES (?, ?)', (ville[0], ville[8]))
        conn.commit()
        # remplir la table villes_departments_regions
        c.execute('INSERT INTO villes_departments_regions (ville_id, department_id, region_id) VALUES (?, ?, ?)', (ville[0], ville[7], ville[8]))
        conn.commit()




    # supprimer la table villes
    c.execute('DROP TABLE villes')
    conn.commit()

    # renommer la table villes2 en villes
    c.execute('ALTER TABLE villes2 RENAME TO villes')
    conn.commit()

# main
if __name__ == '__main__':
    normaliseBase()
