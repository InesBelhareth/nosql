import sqlite3
import json
try:
    from neo4j import GraphDatabase
except ImportError:
    GraphDatabase = None

DB_PATH = "bibliotheque.db"

# Configuration Neo4j
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password" 

# ══════════════════════════════════════════════════════════════════════════════
#  SCHÉMA CYPHER — Contraintes et Index
# ══════════════════════════════════════════════════════════════════════════════

CYPHER_CONSTRAINTS = """
CREATE CONSTRAINT IF NOT EXISTS FOR (l:Livre)      REQUIRE l.livre_id      IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (a:Auteur)     REQUIRE a.auteur_id     IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (m:Membre)     REQUIRE m.membre_id     IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (c:Categorie)  REQUIRE c.nom           IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (e:Exemplaire) REQUIRE e.exemplaire_id IS UNIQUE;

CREATE INDEX IF NOT EXISTS FOR (l:Livre)   ON (l.titre);
CREATE INDEX IF NOT EXISTS FOR (m:Membre)  ON (m.email);
"""

# ══════════════════════════════════════════════════════════════════════════════
#  CHARGEMENT ET GÉNÉRATION
# ══════════════════════════════════════════════════════════════════════════════

def load_data():
    conn = sqlite3.connect(DB_PATH)
    def fetch_all(sql):
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    
    data = {
        "categories":  fetch_all("SELECT * FROM categories"),
        "auteurs":     fetch_all("SELECT * FROM auteurs"),
        "livres":      fetch_all("SELECT * FROM livres"),
        "membres":     fetch_all("SELECT * FROM membres"),
        "exemplaires": fetch_all("SELECT * FROM exemplaires"),
        "emprunts":    fetch_all("SELECT * FROM emprunts"),
    }
    conn.close()
    return data

def build_cypher_nodes(data):
    lines = []
    # Categories
    for c in data["categories"]:
        desc = c["description"].replace("'", "\\'") if c["description"] else ""
        lines.append(f"MERGE (:Categorie {{nom: '{c['nom']}', description: '{desc}'}});")

    # Auteurs
    for a in data["auteurs"]:
        nat = a["nationalite"].replace("'", "\\'") if a["nationalite"] else ""
        lines.append(f"MERGE (:Auteur {{auteur_id: {a['id']}, nom: '{a['nom']}', prenom: '{a['prenom']}', nationalite: '{nat}'}});")

    # Livres
    for l in data["livres"]:
        titre = l["titre"].replace("'", "\\'")
        lines.append(f"MERGE (:Livre {{livre_id: {l['id']}, titre: '{titre}', isbn: '{l['isbn']}', prix: {l['prix']}}});")

    # Membres
    for m in data["membres"]:
        lines.append(f"MERGE (:Membre {{membre_id: {m['id']}, nom: '{m['nom']}', email: '{m['email']}', ville: '{m['ville']}'}});")

    # Exemplaires
    for e in data["exemplaires"]:
        dispo = "true" if e["disponible"] else "false"
        lines.append(f"MERGE (:Exemplaire {{exemplaire_id: {e['id']}, etat: '{e['etat']}', disponible: {dispo}}});")

    return "\n".join(lines)

def build_cypher_relations(data):
    lines = []
    
    # A_ECRIT & APPARTIENT_A
    for l in data["livres"]:
        cat = next(c["nom"] for c in data["categories"] if c["id"] == l["categorie_id"])
        lines.append(f"MATCH (a:Auteur {{auteur_id: {l['auteur_id']}}}), (lv:Livre {{livre_id: {l['id']}}}) MERGE (a)-[:A_ECRIT]->(lv);")
        lines.append(f"MATCH (lv:Livre {{livre_id: {l['id']}}}), (c:Categorie {{nom: '{cat}'}}) MERGE (lv)-[:APPARTIENT_A]->(c);")

    # A_EXEMPLAIRE
    for e in data["exemplaires"]:
        lines.append(f"MATCH (lv:Livre {{livre_id: {e['livre_id']}}}), (ex:Exemplaire {{exemplaire_id: {e['id']}}}) MERGE (lv)-[:A_EXEMPLAIRE]->(ex);")

    # A_EMPRUNTE (The Fixed Logic)
    lines.append("\n// --- Relations A_EMPRUNTE avec gestion des NULLs ---")
    for em in data["emprunts"]:
        date_ret = f"'{em['date_retour_reelle']}'" if em["date_retour_reelle"] else "null"
        
        # We MERGE on the unique ID of the loan relationship
        # We SET properties separately to allow null values
        lines.append(
            f"MATCH (m:Membre {{membre_id: {em['membre_id']}}}), "
            f"(ex:Exemplaire {{exemplaire_id: {em['exemplaire_id']}}})\n"
            f"MERGE (m)-[rel:A_EMPRUNTE {{emprunt_id: {em['id']}}}]->(ex)\n"
            f"ON CREATE SET "
            f"rel.date_emprunt = '{em['date_emprunt']}', "
            f"rel.date_retour_prevue = '{em['date_retour_prevue']}', "
            f"rel.date_retour_reelle = {date_ret}, "
            f"rel.statut = '{em['statut']}';"
        )

    return "\n".join(lines)

# ══════════════════════════════════════════════════════════════════════════════
#  EXÉCUTION
# ══════════════════════════════════════════════════════════════════════════════

def execute_migration_neo4j(data):
    if not GraphDatabase:
        print("[!] Install 'neo4j' library to run.")
        return

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        with driver.session() as session:
            print("Cleanup existing data...")
            session.run("MATCH (n) DETACH DELETE n")
            
            print("Applying constraints...")
            for stmt in CYPHER_CONSTRAINTS.split(';')[:-1]:
                session.run(stmt)

            print("Creating nodes...")
            for stmt in build_cypher_nodes(data).split(';')[:-1]:
                session.run(stmt)

            print("Creating relations (Handling Nulls)...")
            # We split by semicolon but keep the full multi-line MERGE block
            for stmt in build_cypher_relations(data).split(';'):
                if stmt.strip():
                    session.run(stmt)
                    
        print("[OK] Migration successful.")
    except Exception as e:
        print(f"[ERREUR] : {e}")
    finally:
        driver.close()

if __name__ == "__main__":
    dataset = load_data()
    execute_migration_neo4j(dataset)