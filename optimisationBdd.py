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

# normalise la base de données en utilisant la norme 3NF
def normaliseBase():
    ''' normalise la base de données en utilisant la norme 3NF
    '''
    # créer une table villes2 avec les champs id, insee_code, zip_code, name, slug, gps_lat, gps_lng et department_id (foreign key) si elle n'existe pas
    conn = connectBase()
    c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS villes2 (id INTEGER PRIMARY KEY, insee_code TEXT, zip_code TEXT, name TEXT, slug TEXT, gps_lat TEXT, gps_lng TEXT, department_id INTEGER, FOREIGN KEY(department_id) REFERENCES departments(id))')
    conn.commit()

    # créer une table departments avec les champs id, code, name, slug et region_id (foreign key) si elle n'existe pas
    c.execute('CREATE TABLE IF NOT EXISTS departments (id INTEGER PRIMARY KEY, code TEXT, name TEXT, slug TEXT, region_id INTEGER, FOREIGN KEY(region_id) REFERENCES regions(id))')
    conn.commit()

    # créer une table regions avec les champs id, code, name, slug si elle n'existe pas
    c.execute('CREATE TABLE IF NOT EXISTS regions (id INTEGER PRIMARY KEY, code TEXT, name TEXT, slug TEXT)')
    conn.commit()

    # créer une table villes_departments avec les champs id, ville_id (foreign key) et department_id (foreign keys) si elle n'existe pas
    c.execute('CREATE TABLE IF NOT EXISTS villes_departments (id INTEGER PRIMARY KEY, ville_id INTEGER, department_id INTEGER, FOREIGN KEY(ville_id) REFERENCES villes2(id), FOREIGN KEY(department_id) REFERENCES departments(id))')
    conn.commit()

    # créer une table villes_regions avec les champs id, ville_id (foreign key) et region_id (foreign key) si elle n'existe pas
    c.execute('CREATE TABLE IF NOT EXISTS villes_regions (id INTEGER PRIMARY KEY, ville_id INTEGER, region_id INTEGER, FOREIGN KEY(ville_id) REFERENCES villes2(id), FOREIGN KEY(region_id) REFERENCES regions(id))')
    conn.commit()

    # créer une table villes_departments_regions avec les champs id, ville_id (foreign key), department_id (foreign key) et region_id (foreign key) si elle n'existe pas
    c.execute('CREATE TABLE IF NOT EXISTS villes_departments_regions (id INTEGER PRIMARY KEY, ville_id INTEGER, department_id INTEGER, region_id INTEGER, FOREIGN KEY(ville_id) REFERENCES villes2(id), FOREIGN KEY(department_id) REFERENCES departments(id), FOREIGN KEY(region_id) REFERENCES regions(id))')
    conn.commit()

    # remplir les tables
    c.execute('SELECT * FROM villes')
    villes = c.fetchall()

    for ville in villes:
        codeInsee = ville[1]
        zip_code = ville[2]
        name = ville[3]
        slug = ville[4]
        gps_lat = ville[5]
        gps_lng = ville[6]
        department_code = ville[7]
        department_name = ville[8]
        department_slug = ville[9]
        region_code = ville[10]
        region_name = ville[11]
        region_slug = ville[12]
        # verifie si la region existe
        c.execute('SELECT * FROM regions WHERE code = ?', (region_code,))
        region = c.fetchone()
        region_id = region[0] if region else 777
        if region == None:
            c.execute('INSERT INTO regions (code, name, slug) VALUES (?, ?, ?)', (region_code, region_name, region_slug))
            conn.commit()
            region_id = c.lastrowid
        # verifie si le departement existe
        c.execute('SELECT * FROM departments WHERE code = ?', (department_code,))
        department = c.fetchone()
        department_id = department[0] if department else 777
        if department == None:
            c.execute('INSERT INTO departments (code, name, slug, region_id) VALUES (?, ?, ?, ?)', (department_code, department_name, department_slug, region_id))
            conn.commit()
            department_id = c.lastrowid
        # remplir la table villes2
        c.execute('INSERT INTO villes2 (insee_code, zip_code, name, slug, gps_lat, gps_lng, department_id) VALUES (?, ?, ?, ?, ?, ?, ?)', (codeInsee, zip_code, name, slug, gps_lat, gps_lng, department_id))
        # remplir la table villes_departments
        ville_id = c.lastrowid
        c.execute('INSERT INTO villes_departments (ville_id, department_id) VALUES (?, ?)', (ville_id, department_id))
        # remplir la table villes_regions
        c.execute('INSERT INTO villes_regions (ville_id, region_id) VALUES (?, ?)', (ville_id, region_id))
        conn.commit()
        # remplir la table villes_regions
        c.execute('INSERT INTO villes_departments_regions (ville_id, department_id, region_id) VALUES (?, ?, ?)', (ville_id, department_id, region_id))
        conn.commit()
        # remplir la table villes_departments_regions
        c.execute('INSERT INTO villes_departments_regions (ville_id, department_id, region_id) VALUES (?, ?, ?)', (ville_id, department_id, region_id))
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
    print('Base de données normalisée')