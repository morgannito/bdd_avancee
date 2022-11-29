import sqlite3
import timeit


# se connecte aux bases de données
def connectBase():
    ''' return a connector to the database
    '''
    try:
        conn_paris = sqlite3.connect("paris.db")
        conn_bordeaux = sqlite3.connect("bordeaux.db")
        conn_marseille = sqlite3.connect("marseille.db")
        return conn_paris, conn_bordeaux, conn_marseille
    except:
        return False

# simule l'ajoute ou la modification d'une region
def addRegion(code, name,slug):
    ''' add a region in the database
    '''
    conn_paris, conn_bordeaux, conn_marseille = connectBase()
    cursor_paris = conn_paris.cursor()
    cursor_paris.execute("INSERT INTO regions (code,name,slug) VALUES (?, ?,?)", (code, name,slug))
    conn_paris.commit()
    # ecrire le code de la requete sql dans le fichier txt paris.txt
    f = open("paris.txt", "w")
    f.write("INSERT INTO regions (code,name,slug) VALUES ("+code+", "+name+", "+slug+")")
    f.close()
    # print la requete sql dans le terminal
    print("INSERT INTO regions (code,name,slug) VALUES ("+code+", "+name+", "+slug+")")
# simule la suppression d'une region
def deleteRegion(code):
    ''' delete a region in the database
    '''
    conn_paris, conn_bordeaux, conn_marseille = connectBase()
    cursor_paris = conn_paris.cursor()
    cursor_paris.execute("DELETE FROM regions WHERE code = ?", (code,))
    conn_paris.commit()
    # ecrire le code de la requete sql dans le fichier txt paris.txt
    f = open("paris.txt", "w")
    f.write("DELETE FROM regions WHERE code = "+code)
    f.close()
    # print la requete sql dans le terminal
    print("DELETE FROM regions WHERE code = "+code)

# simule la modification d'une region
def updateRegion(code, name,slug):
    ''' update a region in the database
    '''
    conn_paris, conn_bordeaux, conn_marseille = connectBase()
    cursor_paris = conn_paris.cursor()
    cursor_paris.execute("UPDATE regions SET name = ?, slug = ? WHERE code = ?", (name,slug,code))
    conn_paris.commit()
    # ecrire le code de la requete sql dans le fichier txt paris.txt
    f = open("paris.txt", "w")
    f.write("UPDATE regions SET name = "+name+", slug = "+slug+" WHERE code = "+code)
    f.close()
    # print la requete sql dans le terminal
    print("UPDATE regions SET name = "+name+", slug = "+slug+" WHERE code = "+code)


# ajoute une tuple la table départements a la  base de données marseille
def addDepartement(code, name,slug,region_code):
    ''' add a departement in the database
    '''
    conn_paris, conn_bordeaux, conn_marseille = connectBase()
    cursor_marseille = conn_marseille.cursor()
    cursor_marseille.execute("INSERT INTO departments (code,name,slug,region_id) VALUES (?, ?,?,?)", (code, name,slug,region_code))
    conn_marseille.commit()
    # ecrire le code de la requete sql dans le fichier txt marseille.txt
    f = open("marseille.txt", "w")
    f.write("INSERT INTO departments (code,name,slug,region_id) VALUES ("+code+", "+name+", "+slug+", "+region_code+")")
    f.close()
    # print la requete sql dans le terminal
    print("INSERT INTO departments (code,name,slug,region_id) VALUES ("+code+", "+name+", "+slug+", "+region_code+")")

# modifie la table departments de la base de données marseille
def updateDepartement(code, name,slug,region_code):
    ''' update a departement in the database
    '''
    conn_paris, conn_bordeaux, conn_marseille = connectBase()
    cursor_marseille = conn_marseille.cursor()
    cursor_marseille.execute("UPDATE departments SET name = ?, slug = ?, region_id = ? WHERE code = ?", (name,slug,region_code,code))
    conn_marseille.commit()
    # ecrire le code de la requete sql dans le fichier txt marseille.txt
    f = open("marseille.txt", "w")
    f.write("UPDATE departments SET name = "+name+", slug = "+slug+", region_id = "+region_code+" WHERE code = "+code)
    f.close()
    # print la requete sql dans le terminal
    print("UPDATE departments SET name = "+name+", slug = "+slug+", region_id = "+region_code+" WHERE code = "+code)

# delete une tuple de la table departments de la base de données marseille
def deleteDepartement(code):
    ''' delete a departement in the database
    '''
    conn_paris, conn_bordeaux, conn_marseille = connectBase()
    cursor_marseille = conn_marseille.cursor()
    cursor_marseille.execute("DELETE FROM departments WHERE code = ?", (code,))
    conn_marseille.commit()
    # ecrire le code de la requete sql dans le fichier txt marseille.txt
    f = open("marseille.txt", "w")
    f.write("DELETE FROM departments WHERE code = "+code)
    f.close()
    # print la requete sql dans le terminal
    print("DELETE FROM departments WHERE code = "+code)

# ajoute une tuple la table villes a la  base de données bordeaux
def addCity(insee_code,zip_code, name,slug,gps_lat,gps_lng,department_id):
    ''' add a city in the database
    '''
    conn_paris, conn_bordeaux, conn_marseille = connectBase()
    cursor_bordeaux = conn_bordeaux.cursor()
    cursor_bordeaux.execute("INSERT INTO villes (insee_code,zip_code,name,slug,gps_lat,gps_lng,department_id) VALUES (?, ?,?,?,?,?,?)", (insee_code,zip_code, name,slug,gps_lat,gps_lng,department_id))
    conn_bordeaux.commit()
    # ecrire le code de la requete sql dans le fichier txt bordeaux.txt
    f = open("bordeaux.txt", "w")
    f.write("INSERT INTO villes (insee_code,zip_code,name,slug,gps_lat,gps_lng,department_id) VALUES ("+insee_code+", "+zip_code+", "+name+", "+slug+", "+gps_lat+", "+gps_lng+", "+department_id+")")
    f.close()
    # print la requete sql dans le terminal
    print("INSERT INTO villes (insee_code,zip_code,name,slug,gps_lat,gps_lng,department_id) VALUES ("+insee_code+", "+zip_code+", "+name+", "+slug+", "+gps_lat+", "+gps_lng+", "+department_id+")")

# modifie la table villes de la base de données bordeaux
def updateCity(insee_code,zip_code, name,slug,gps_lat,gps_lng,department_id):
    ''' update a city in the database
    '''
    conn_paris, conn_bordeaux, conn_marseille = connectBase()
    cursor_bordeaux = conn_bordeaux.cursor()
    cursor_bordeaux.execute("UPDATE villes SET zip_code = ?, name = ?, slug = ?, gps_lat = ?, gps_lng = ?, department_id = ? WHERE insee_code = ?", (zip_code,name,slug,gps_lat,gps_lng,department_id,insee_code))
    conn_bordeaux.commit()
    # ecrire le code de la requete sql dans le fichier txt bordeaux.txt
    f = open("bordeaux.txt", "w")
    f.write("UPDATE villes SET zip_code = "+zip_code+", name = "+name+", slug = "+slug+", gps_lat = "+gps_lat+", gps_lng = "+gps_lng+", department_id = "+department_id+" WHERE insee_code = "+insee_code)
    f.close()
    # print la requete sql dans le terminal
    print("UPDATE villes SET zip_code = "+zip_code+", name = "+name+", slug = "+slug+", gps_lat = "+gps_lat+", gps_lng = "+gps_lng+", department_id = "+department_id+" WHERE insee_code = "+insee_code)

# delete une tuple de la table villes de la base de données bordeaux
def deleteCity(insee_code):
    ''' delete a city in the database
    '''
    conn_paris, conn_bordeaux, conn_marseille = connectBase()
    cursor_bordeaux = conn_bordeaux.cursor()
    cursor_bordeaux.execute("DELETE FROM villes WHERE insee_code = ?", (insee_code,))
    conn_bordeaux.commit()
    # ecrire le code de la requete sql dans le fichier txt bordeaux.txt
    f = open("bordeaux.txt", "w")
    f.write("DELETE FROM villes WHERE insee_code = "+insee_code)
    f.close()
    # print la requete sql dans le terminal
    print("DELETE FROM villes WHERE insee_code = "+insee_code)


# replica sur les autres bases de données
def replica_paris():
    ''' replica the region table from paris to bordeaux and marseille
    '''
    # regarde le fichier paris.txt et pour chaque ligne, execute la requete sql et supprime la ligne du fichier
    conn_paris, conn_bordeaux, conn_marseille = connectBase()
    f = open("paris.txt", "r")
    for line in f:
        cursor_bordeaux = conn_bordeaux.cursor()
        cursor_marseille = conn_marseille.cursor()
        cursor_bordeaux.execute(line)
        cursor_marseille.execute(line)
        conn_bordeaux.commit()
        conn_marseille.commit()
    f = open("paris.txt", "w")
    f.write("")
    f.close()
    print("mise a jour des bases de données region effectuée")

def replica_marseille():
    ''' replica the departement table from marseille to bordeaux and paris
    '''
    # regarde le fichier marseille.txt et pour chaque ligne, execute la requete sql et supprime la ligne du fichier
    conn_paris, conn_bordeaux, conn_marseille = connectBase()
    f = open("marseille.txt", "r")
    for line in f:
        cursor_bordeaux = conn_bordeaux.cursor()
        cursor_paris = conn_paris.cursor()
        cursor_bordeaux.execute(line)
        cursor_paris.execute(line)
        conn_bordeaux.commit()
        conn_paris.commit()
    f = open("marseille.txt", "w")
    f.write("")
    f.close()
    print("mise a jour des bases de données departement effectuée")


# main
if __name__ == "__main__":
    print("")
    # # ajoute une region
    # addRegion("1554544", "region1","region1")
    # updateRegion("1554544", "region1","region1")
    # deleteRegion("1554544")
    # addRegion("1554544", "region1", "region1")
    # updateRegion("1554544", "region1", "region1")
    # deleteRegion("1554544")
    # addRegion("1554544", "region1", "region1")
    # updateRegion("1554544", "region1", "region1")
    # deleteRegion("1554544")
    # replica_paris()
    #
    # # ajoute un departement
    # addDepartement("1554544", "departement1", "departement1", "1554544")
    # updateDepartement("1554544", "departement1", "departement1", "1554544")
    # deleteDepartement("1554544")
    # addDepartement("1554544", "departement1", "departement1", "1554544")
    # updateDepartement("1554544", "departement1", "departement1", "1554544")
    # deleteDepartement("1554544")
    #
    # # ajoute une ville
    # addCity("1554544", "1554544", "ville1", "ville1", "1554544", "1554544", "1554544")
    # updateCity("1554544", "1554544", "ville1", "ville1", "1554544", "1554544", "1554544")
    # deleteCity("1554544")
    # addCity("1554544", "1554544", "ville1", "ville1", "1554544", "1554544", "1554544")
    # updateCity("1554544", "1554544", "ville1", "ville1", "1554544", "1554544", "1554544")
    # deleteCity("1554544")

# todo le faire en plus propre
# todo faire un programme qui fait tout ca automatiquement desqu'il y a une modification sur le fichier paris.txt et marseille.txt et bordeaux.txt

# fait un select sur les tables de la base de données bordeaux,paris,marseille
def select(city):
    conn_paris, conn_bordeaux, conn_marseille = connectBase()
    cursor_bordeaux = conn_bordeaux.cursor()
    cursor_paris = conn_paris.cursor()
    cursor_marseille = conn_marseille.cursor()
    cursor_bordeaux.execute("SELECT * FROM villes")
    cursor_paris.execute("SELECT * FROM regions")
    cursor_marseille.execute("SELECT * FROM departments")
    # pour chaque villes de la base de données bordeaux va chercher les infos de la region et du departement correspondant
    for row in cursor_bordeaux:
        if row[4] == city:
            for row2 in cursor_marseille:
                if row2[0] == row[7]:
                    for row3 in cursor_paris:
                        if row3[0] == row2[4]:
                            print(row[1], row[2], row[3], row[4], row[5], row[6], row[7], row2[1], row2[2], row2[3], row3[1], row3[2], row3[3])
select("perpignan")


