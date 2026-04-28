import os
import decimal
import mysql.connector

# This handles the Python 3.12+ asyncore removal automatically 
# once you 'pip install pyasyncore'
try:
    import pyasyncore as asyncore
except ImportError:
    import asyncore

# Force the driver to use Python-based implementation for compatibility
os.environ['CASS_DRIVER_NO_EXTENSIONS'] = '1'

from cassandra.cluster import Cluster
from cassandra.io.asyncioreactor import AsyncioConnection

# ── CONFIGURATION ─────────────────────────────────────────────────────────────

MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'bibliotheque'
}

CASSANDRA_HOSTS = ['127.0.0.1']
CASSANDRA_KEYSPACE = 'bibliotheque'

# ── SCHÉMA CQL ────────────────────────────────────────────────────────────────

CQL_TABLES = [
    """
    CREATE TABLE IF NOT EXISTS livres_par_categorie (
        categorie TEXT, titre TEXT, livre_id INT, isbn TEXT, annee_pub INT,
        editeur TEXT, prix DECIMAL, auteur_nom TEXT, auteur_prenom TEXT,
        auteur_nationalite TEXT, nb_exemplaires INT, nb_disponibles INT,
        PRIMARY KEY ((categorie), titre, livre_id)
    ) WITH CLUSTERING ORDER BY (titre ASC);
    """,
    """
    CREATE TABLE IF NOT EXISTS livres_par_auteur (
        auteur_nom TEXT, auteur_prenom TEXT, titre TEXT, livre_id INT, isbn TEXT,
        annee_pub INT, editeur TEXT, prix DECIMAL, categorie TEXT, nb_disponibles INT,
        PRIMARY KEY ((auteur_nom, auteur_prenom), titre, livre_id)
    ) WITH CLUSTERING ORDER BY (titre ASC);
    """,
    """
    CREATE TABLE IF NOT EXISTS emprunts_par_membre (
        membre_id INT, membre_email TEXT, date_emprunt TEXT, emprunt_id INT,
        livre_titre TEXT, livre_isbn TEXT, exemplaire_id INT, date_retour_prevue TEXT,
        date_retour_reelle TEXT, statut TEXT,
        PRIMARY KEY ((membre_id), date_emprunt, emprunt_id)
    ) WITH CLUSTERING ORDER BY (date_emprunt DESC);
    """,
    """
    CREATE TABLE IF NOT EXISTS disponibilite_livres (
        livre_id INT, titre TEXT, exemplaire_id INT, etat TEXT, disponible BOOLEAN,
        PRIMARY KEY ((livre_id), exemplaire_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS emprunts_en_retard (
        statut TEXT, date_retour_prevue TEXT, emprunt_id INT, membre_nom TEXT,
        membre_prenom TEXT, membre_email TEXT, livre_titre TEXT, date_emprunt TEXT,
        PRIMARY KEY ((statut), date_retour_prevue, emprunt_id)
    ) WITH CLUSTERING ORDER BY (date_retour_prevue ASC);
    """,
    """
    CREATE TABLE IF NOT EXISTS membres_par_ville (
        ville TEXT, nom TEXT, prenom TEXT, membre_id INT, email TEXT,
        telephone TEXT, adresse TEXT, date_inscription TEXT,
        PRIMARY KEY ((ville), nom, prenom, membre_id)
    ) WITH CLUSTERING ORDER BY (nom ASC, prenom ASC);
    """
]

# ── LOGIQUE DE MIGRATION ──────────────────────────────────────────────────────

def fetch_mysql(sql):
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor(dictionary=True)
    cur.execute(sql)
    res = cur.fetchall()
    conn.close()
    return res

def migrate():
    print("🚀 Démarrage de la migration...")

    # 1. Extraction (Using dicts for O(1) lookups)
    try:
        categories = {r["id"]: r for r in fetch_mysql("SELECT * FROM categories")}
        auteurs = {r["id"]: r for r in fetch_mysql("SELECT * FROM auteurs")}
        livres = fetch_mysql("SELECT * FROM livres")
        livres_dict = {l["id"]: l for l in livres}
        membres = fetch_mysql("SELECT * FROM membres")
        membres_dict = {m["id"]: m for m in membres}
        exemplaires = fetch_mysql("SELECT * FROM exemplaires")
        emprunts = fetch_mysql("SELECT * FROM emprunts")
    except Exception as e:
        print(f"❌ Erreur MySQL: {e}")
        return

    # 2. Connexion Cassandra
    try:
        cluster = Cluster(CASSANDRA_HOSTS, connection_class=AsyncioConnection)
        session = cluster.connect()
    except Exception as e:
        print(f"❌ Erreur Cassandra: {e}")
        return

    # 3. Schéma
    session.execute(f"CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}};")
    session.set_keyspace(CASSANDRA_KEYSPACE)
    for table in CQL_TABLES:
        session.execute(table)

    # 4. Insertion
    print("📤 Migration des données en cours...")

    # -- Livres & Categories --
    p1 = session.prepare("INSERT INTO livres_par_categorie (categorie, titre, livre_id, isbn, annee_pub, editeur, prix, auteur_nom, auteur_prenom, auteur_nationalite, nb_exemplaires, nb_disponibles) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")
    p2 = session.prepare("INSERT INTO livres_par_auteur (auteur_nom, auteur_prenom, titre, livre_id, isbn, annee_pub, editeur, prix, categorie, nb_disponibles) VALUES (?,?,?,?,?,?,?,?,?,?)")
    
    for l in livres:
        a = auteurs.get(l["auteur_id"], {})
        cat = categories.get(l["categorie_id"], {'nom': 'N/A'})
        exs = [e for e in exemplaires if e["livre_id"] == l["id"]]
        p = decimal.Decimal(str(l["prix"])) if l["prix"] else decimal.Decimal('0.00')
        
        session.execute(p1, (cat['nom'], l['titre'], l['id'], l['isbn'], l['annee_pub'], l['editeur'], p, a.get('nom'), a.get('prenom'), a.get('nationalite'), len(exs), sum(1 for e in exs if e['disponible'])))
        session.execute(p2, (a.get('nom'), a.get('prenom'), l['titre'], l['id'], l['isbn'], l['annee_pub'], l['editeur'], p, cat['nom'], sum(1 for e in exs if e['disponible'])))

    # -- Membres --
    p6 = session.prepare("INSERT INTO membres_par_ville (ville, nom, prenom, membre_id, email, telephone, adresse, date_inscription) VALUES (?,?,?,?,?,?,?,?)")
    for m in membres:
        session.execute(p6, (m['ville'], m['nom'], m['prenom'], m['id'], m['email'], m['telephone'], m['adresse'], str(m['date_inscription'])))

    # -- Exemplaires --
    p4 = session.prepare("INSERT INTO disponibilite_livres (livre_id, titre, exemplaire_id, etat, disponible) VALUES (?,?,?,?,?)")
    for e in exemplaires:
        l = livres_dict.get(e["livre_id"])
        if l: session.execute(p4, (l['id'], l['titre'], e['id'], e['etat'], bool(e['disponible'])))

    # -- Emprunts --
    p3 = session.prepare("INSERT INTO emprunts_par_membre (membre_id, membre_email, date_emprunt, emprunt_id, livre_titre, livre_isbn, exemplaire_id, date_retour_prevue, date_retour_reelle, statut) VALUES (?,?,?,?,?,?,?,?,?,?)")
    p5 = session.prepare("INSERT INTO emprunts_en_retard (statut, date_retour_prevue, emprunt_id, membre_nom, membre_prenom, membre_email, livre_titre, date_emprunt) VALUES (?,?,?,?,?,?,?,?)")
    
    ex_dict = {e["id"]: e for e in exemplaires}
    for em in emprunts:
        ex = ex_dict.get(em["exemplaire_id"])
        l = livres_dict.get(ex["livre_id"]) if ex else None
        m = membres_dict.get(em["membre_id"])
        if not (l and m): continue

        dates = [str(em["date_emprunt"]), str(em["date_retour_prevue"]), str(em["date_retour_reelle"]) if em["date_retour_reelle"] else None]
        session.execute(p3, (m['id'], m['email'], dates[0], em['id'], l['titre'], l['isbn'], ex['id'], dates[1], dates[2], em['statut']))
        
        if em["statut"] in ("retard", "en_cours"):
            session.execute(p5, (em['statut'], dates[1], em['id'], m['nom'], m['prenom'], m['email'], l['titre'], dates[0]))

    print("✅ Migration terminée !")
    cluster.shutdown()

if __name__ == "__main__":
    migrate()