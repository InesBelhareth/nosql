"""
01_base_relationnelle.py
Base de données relationnelle - Système de Gestion de Bibliothèque (MySQL)
"""
import mysql.connector

# Configuration de la base de données MySQL
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'bibliotheque'
}

def create_database():
    # 1. Connexion au serveur MySQL pour créer la base si elle n'existe pas
    conn = mysql.connector.connect(
        host=DB_CONFIG['host'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
    conn.close()

    # 2. Connexion à la base de données spécifique
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # ── Tables ──────────────────────────────────────────────────
    schema = """
    SET FOREIGN_KEY_CHECKS = 0;

    DROP TABLE IF EXISTS emprunts;
    DROP TABLE IF EXISTS exemplaires;
    DROP TABLE IF EXISTS livres;
    DROP TABLE IF EXISTS auteurs;
    DROP TABLE IF EXISTS membres;
    DROP TABLE IF EXISTS categories;

    CREATE TABLE categories (
        id INT AUTO_INCREMENT PRIMARY KEY,
        nom VARCHAR(255) NOT NULL UNIQUE,
        description TEXT
    );

    CREATE TABLE auteurs (
        id INT AUTO_INCREMENT PRIMARY KEY,
        nom VARCHAR(255) NOT NULL,
        prenom VARCHAR(255) NOT NULL,
        nationalite VARCHAR(255),
        date_naissance DATE
    );

    CREATE TABLE livres (
        id INT AUTO_INCREMENT PRIMARY KEY,
        titre VARCHAR(255) NOT NULL,
        isbn VARCHAR(255) UNIQUE,
        annee_pub INT,
        editeur VARCHAR(255),
        prix DECIMAL(10,2),
        auteur_id INT,
        categorie_id INT,
        FOREIGN KEY (auteur_id) REFERENCES auteurs(id),
        FOREIGN KEY (categorie_id) REFERENCES categories(id)
    );

    CREATE TABLE membres (
        id INT AUTO_INCREMENT PRIMARY KEY,
        nom VARCHAR(255) NOT NULL,
        prenom VARCHAR(255) NOT NULL,
        email VARCHAR(255) UNIQUE,
        telephone VARCHAR(255),
        adresse TEXT,
        ville VARCHAR(255),
        date_inscription DATE
    );

    CREATE TABLE exemplaires (
        id INT AUTO_INCREMENT PRIMARY KEY,
        livre_id INT,
        etat ENUM('bon','use','endommage'),
        disponible INT DEFAULT 1,
        FOREIGN KEY (livre_id) REFERENCES livres(id)
    );

    CREATE TABLE emprunts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        membre_id INT,
        exemplaire_id INT,
        date_emprunt DATE NOT NULL,
        date_retour_prevue DATE NOT NULL,
        date_retour_reelle DATE,
        statut ENUM('en_cours','rendu','retard'),
        FOREIGN KEY (membre_id) REFERENCES membres(id),
        FOREIGN KEY (exemplaire_id) REFERENCES exemplaires(id)
    );

    SET FOREIGN_KEY_CHECKS = 1;
    """
    
    # Exécution des requêtes de création de schéma
    for statement in schema.split(';'):
        statement = statement.strip()
        if statement:
            cursor.execute(statement)

    # ── Données ─────────────────────────────────────────────────
    categories = [
        ("Informatique",   "Livres sur la programmation et les systèmes"),
        ("Sciences",       "Mathématiques, physique, biologie"),
        ("Littérature",    "Romans, nouvelles, poésie"),
        ("Histoire",       "Histoire mondiale et locale"),
        ("Philosophie",    "Pensée critique et philosophie"),
    ]
    cursor.executemany("INSERT INTO categories(nom,description) VALUES(%s,%s)", categories)

    auteurs = [
        ("Knuth",      "Donald",   "Américain",  "1938-01-10"),
        ("Martin",     "Robert",   "Américain",  "1952-12-05"),
        ("Codd",       "Edgar",    "Britannique","1923-08-19"),
        ("Camus",      "Albert",   "Français",   "1913-11-07"),
        ("Tanenbaum",  "Andrew",   "Américain",  "1944-03-16"),
        ("Fowler",     "Martin",   "Britannique","1963-06-18"),
        ("García M.",  "Gabriel",  "Colombien",  "1927-03-06"),
        ("Nietzsche",  "Friedrich","Allemand",   "1844-10-15"),
    ]
    cursor.executemany(
        "INSERT INTO auteurs(nom,prenom,nationalite,date_naissance) VALUES(%s,%s,%s,%s)",
        auteurs
    )

    livres = [
        ("The Art of Computer Programming", "978-0-201-03801-0", 1968, "Addison-Wesley", 89.99, 1, 1),
        ("Clean Code",                      "978-0-13-235088-4", 2008, "Prentice Hall",  45.00, 2, 1),
        ("The Relational Model for DB",     "978-0-201-14192-4", 1990, "Addison-Wesley", 55.00, 3, 1),
        ("L'Étranger",                      "978-2-07-036024-5", 1942, "Gallimard",      12.00, 4, 3),
        ("Modern Operating Systems",        "978-0-13-359162-0", 2014, "Pearson",        70.00, 5, 1),
        ("Refactoring",                     "978-0-13-468599-1", 1999, "Addison-Wesley", 50.00, 6, 1),
        ("Cent ans de solitude",            "978-2-07-036088-7", 1967, "Gallimard",      14.00, 7, 3),
        ("Ainsi parlait Zarathoustra",      "978-2-07-041217-5", 1885, "Gallimard",      11.00, 8, 5),
        ("La Chute",                        "978-2-07-036026-9", 1956, "Gallimard",      10.00, 4, 3),
        ("Patterns of Enterprise App Arch", "978-0-32-112521-7", 2002, "Addison-Wesley", 60.00, 6, 1),
    ]
    cursor.executemany(
        "INSERT INTO livres(titre,isbn,annee_pub,editeur,prix,auteur_id,categorie_id) VALUES(%s,%s,%s,%s,%s,%s,%s)",
        livres
    )

    membres = [
        ("Ben Ali",   "Mohamed",  "m.benali@email.com",    "+216 71 000 001", "Rue Habib Bourguiba 12", "Tunis",   "2022-01-15"),
        ("Chaabane",  "Fatima",   "f.chaabane@email.com",  "+216 71 000 002", "Avenue Farhat Hached 5", "Sfax",    "2022-03-20"),
        ("Trabelsi",  "Youssef",  "y.trabelsi@email.com",  "+216 71 000 003", "Rue de la Liberté 8",    "Sousse",  "2022-06-10"),
        ("Mansouri",  "Leila",    "l.mansouri@email.com",  "+216 71 000 004", "Avenue de Carthage 22",  "Tunis",   "2023-01-05"),
        ("Brahmi",    "Karim",    "k.brahmi@email.com",    "+216 71 000 005", "Rue Ibn Khaldoun 3",     "Bizerte", "2023-04-18"),
        ("Sassi",     "Amira",    "a.sassi@email.com",     "+216 71 000 006", "Boulevard de l'Indep 7", "Nabeul",  "2023-07-22"),
        ("Mejri",     "Sofien",   "s.mejri@email.com",     "+216 71 000 007", "Rue des Roses 15",       "Tunis",   "2023-09-30"),
        ("Khelifi",   "Nadia",    "n.khelifi@email.com",   "+216 71 000 008", "Avenue de l'Armée 18",   "Gabes",   "2024-01-12"),
    ]
    cursor.executemany(
        "INSERT INTO membres(nom,prenom,email,telephone,adresse,ville,date_inscription) VALUES(%s,%s,%s,%s,%s,%s,%s)",
        membres
    )

    # 2 exemplaires pour les 10 livres
    exemplaires = [(i, etat, disp) for i in range(1,11)
                   for etat, disp in [("bon",1),("bon",1)]]
    exemplaires[0]  = (1, "bon",  0)   # exemplaire 1 du livre 1 = emprunté
    exemplaires[2]  = (2, "use",  0)
    exemplaires[4]  = (3, "bon",  0)
    exemplaires[8]  = (5, "bon",  0)
    cursor.executemany(
        "INSERT INTO exemplaires(livre_id,etat,disponible) VALUES(%s,%s,%s)",
        exemplaires
    )

    emprunts = [
        (1, 1,  "2024-03-01","2024-03-15","2024-03-14","rendu"),
        (2, 3,  "2024-03-05","2024-03-19",None,          "en_cours"),
        (3, 5,  "2024-02-20","2024-03-05",None,          "retard"),
        (4, 9,  "2024-03-10","2024-03-24",None,          "en_cours"),
        (5, 2,  "2024-03-12","2024-03-26","2024-03-25","rendu"),
        (6, 4,  "2024-02-28","2024-03-13",None,          "retard"),
        (7, 6,  "2024-03-15","2024-03-29",None,          "en_cours"),
        (8, 8,  "2024-03-18","2024-04-01",None,          "en_cours"),
    ]
    cursor.executemany(
        "INSERT INTO emprunts(membre_id,exemplaire_id,date_emprunt,date_retour_prevue,date_retour_reelle,statut) VALUES(%s,%s,%s,%s,%s,%s)",
        emprunts
    )

    conn.commit()
    conn.close()
    print("[OK] Base relationnelle MySQL créée :", DB_CONFIG['database'])

def show_stats():
    conn = mysql.connector.connect(**DB_CONFIG)
    cur  = conn.cursor()
    for table in ["categories","auteurs","livres","membres","exemplaires","emprunts"]:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"  {table:15s} : {cur.fetchone()[0]} enregistrements")
    conn.close()

if __name__ == "__main__":
    create_database()
    show_stats()
