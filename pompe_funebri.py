import mariadb

def connessione(check=False):
    host = "127.0.0.1"
    user = "root"
    password = "admin"
    database = "pompe_funebri"
    port = 3306
    
    # Connessione al database
    db = mariadb.connect(host=host, user=user, password=password, database=database, port=port)
    cursor = db.cursor()
    
    if check:
        sql = "SELECT VERSION()"
        cursor.execute(sql)
        data = cursor.fetchone() 
        print("Mi connetto al database %s" % database)
        print("Versione: %s" % data)
        print()
    
    return db, cursor

def disconnessione(connessione):
    cursore, db = connessione
    cursore.close()
    db.close()
    print("Disconnessione dal database")

def eseguiquery(connessione, query, args=None, commit=False):
    cursore, db = connessione
    
    if args is None:
        cursore.execute(query)
    else:
        cursore.execute(query, args)
    
    if commit:
        db.commit()
        risultato = cursore.lastrowid
    else:
        risultato = cursore.fetchall()
    
    return risultato

def creaMorto(connessione):
    query = """CREATE TABLE IF NOT EXISTS Morto(
        idMorto BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
        nome VARCHAR(255) NOT NULL,
        cognome VARCHAR(255) NOT NULL,
        data_nascita CHAR(10) NOT NULL,
        data_morte CHAR(10) NOT NULL,
        gender CHAR(1) NOT NULL,
        PRIMARY KEY(idMorto)
    )"""
    
    return eseguiquery(connessione, query, commit=True)

def creaBara(connessione):
    query = """CREATE TABLE IF NOT EXISTS Bara(
        idBara BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
        nome VARCHAR(255) NOT NULL,
        materiale VARCHAR(255) NOT NULL,
        colore VARCHAR(255) NOT NULL,
        costo DOUBLE,
        PRIMARY KEY(idBara)
    )"""
    
    return eseguiquery(connessione, query, commit=True)

def creaCarro(connessione):
    query = """CREATE TABLE IF NOT EXISTS Carro(
        idCarro BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
        modello VARCHAR(255) NOT NULL,
        marca VARCHAR(255) NOT NULL,
        colore VARCHAR(255) NOT NULL,
        costo DOUBLE,
        PRIMARY KEY(idCarro)
    )"""
    
    return eseguiquery(connessione, query, commit=True)

def creaCliente(connessione):
    query = """CREATE TABLE IF NOT EXISTS Cliente(
        idCliente BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
        nome VARCHAR(255) NOT NULL,
        cognome VARCHAR(255) NOT NULL,
        relazione VARCHAR(255) NOT NULL,
        codice_fiscale VARCHAR(16),
        numero_telefono VARCHAR(10),
        idMorto BIGINT(20) UNSIGNED NOT NULL,
        PRIMARY KEY(idCliente),
        FOREIGN KEY(idMorto) REFERENCES Morto(idMorto)
    )"""
    
    return eseguiquery(connessione, query, commit=True)

def creaContratto(connessione):
    query = """CREATE TABLE IF NOT EXISTS Contratto(
        idContratto BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
        idCarro BIGINT(20) UNSIGNED NOT NULL,
        idMorto BIGINT(20) UNSIGNED NOT NULL,
        idCliente BIGINT(20) UNSIGNED NOT NULL,
        costobase DOUBLE UNSIGNED,
        PRIMARY KEY(idContratto),
        FOREIGN KEY(idCarro) REFERENCES Carro(idCarro),
        FOREIGN KEY(idMorto) REFERENCES Morto(idMorto),
        FOREIGN KEY(idCliente) REFERENCES Cliente(idCliente)
    )"""
    
    return eseguiquery(connessione, query, commit=True)

def inizializza(connessione):
    # Senza FK
    creaMorto(connessione)
    creaBara(connessione)
    creaCarro(connessione)
    
    creaCliente(connessione)
    creaContratto(connessione)
    

def CreaSQL():
    conn = connessione()
    inizializza(conn)
    disconnessione(conn)
    
######################################################################

def addMorto(connessione, nome, cognome, data_nascita, data_morte, gender):
    query = """INSERT INTO Morto (
        nome, 
        cognome, 
        data_nascita, 
        data_morte, 
        gender
    ) VALUES (%s, %s, %s, %s, %s)"""
        
    args = (nome, cognome, data_nascita, data_morte, gender)
    
    return eseguiquery(connessione, query, args, commit=True)

def addBare(connessione, nome, materiale, colore, costo):
    query = """INSERT INTO Bara (
        nome, 
        materiale, 
        colore, 
        costo
    ) VALUES (%s, %s, %s, %s)"""
        
    args = (nome, materiale, colore, costo)
    
    return eseguiquery(connessione, query, args, commit=True)

def addCarro(connessione, modello, marca, colore, costo):
    query = """INSERT INTO Carro (
        modello, 
        marca, 
        colore, 
        costo
    ) VALUES (%s, %s, %s, %s)"""
        
    args = (modello, marca, colore, costo)
    
    return eseguiquery(connessione, query, args, commit=True)

def addCliente(connessione, nome, cognome, relazione, codice_fiscale, numero_telefono, idMorto):
    query = """INSERT INTO Cliente (
        nome, 
        cognome, 
        relazione, 
        codice_fiscale, 
        numero_telefono, 
        idMorto
    ) VALUES (%s, %s, %s, %s, %s, %s)"""
        
    args = (nome, cognome, relazione, codice_fiscale, numero_telefono, idMorto)
    
    return eseguiquery(connessione, query, args, commit=True)

def addContratto(connessione, idCarro, idMorto, idBara, idCliente, costobase):
    query = """INSERT INTO Contratto (
        idCarro, 
        idMorto,
        idBara,
        idCliente, 
        costobase
    ) VALUES (%s, %s, %s, %s, %s)"""  
        
    args = (idCarro, idMorto, idBara, idCliente, costobase)  
    
    return eseguiquery(connessione, query, args, commit=True)


def popolaSQL():
    conn = connessione()
    id_morto = addMorto(conn, 'Ciccio', 'Cane', '1900-01-01', '2000-01-01', 'M')
    id_bara = addBare(conn, 'Bara1', 'Mogano', 'Rosso', 100)
    id_carro = addCarro(conn, 'Carro1', 'Fiat', 'Blu', 200)
    id_cliente = addCliente(conn, 'Alberto', 'Cane', 'nipote', '111111111111111', '1234567890', id_morto)
    addContratto(conn, id_carro, id_morto, id_bara, id_cliente, 300)  # Modifica: Aggiunto il parametro id_bara
    disconnessione(conn)


if __name__ == "__main__":
    CreaSQL()
