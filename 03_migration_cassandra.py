"""
03_migration_cassandra.py
Migration vers Apache Cassandra
Stratégie : modélisation orientée requêtes (Query-Driven Design)
"""
import sqlite3
import json

DB_PATH = "bibliotheque.db"

# ══════════════════════════════════════════════════════════════════════════════
#  SCHÉMA CQL — Cassandra Query Language
# ══════════════════════════════════════════════════════════════════════════════

CQL_KEYSPACE = """
CREATE KEYSPACE IF NOT EXISTS bibliotheque
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 3
}
AND DURABLE_WRITES = true;

USE bibliotheque;
"""

# Table 1 : Recherche par catégorie + titre (Q1)
CQL_TABLE_LIVRES_PAR_CATEGORIE = """
-- Q1 : "Quels livres sont dans la catégorie X ?"
CREATE TABLE IF NOT EXISTS livres_par_categorie (
    categorie        TEXT,
    titre            TEXT,
    livre_id         INT,
    isbn             TEXT,
    annee_pub        INT,
    editeur          TEXT,
    prix             DECIMAL,
    auteur_nom       TEXT,
    auteur_prenom    TEXT,
    auteur_nationalite TEXT,
    nb_exemplaires   INT,
    nb_disponibles   INT,
    PRIMARY KEY ((categorie), titre, livre_id)
) WITH CLUSTERING ORDER BY (titre ASC);
"""

# Table 2 : Recherche par auteur (Q2)
CQL_TABLE_LIVRES_PAR_AUTEUR = """
-- Q2 : "Quels livres a écrit l'auteur X ?"
CREATE TABLE IF NOT EXISTS livres_par_auteur (
    auteur_nom       TEXT,
    auteur_prenom    TEXT,
    titre            TEXT,
    livre_id         INT,
    isbn             TEXT,
    annee_pub        INT,
    editeur          TEXT,
    prix             DECIMAL,
    categorie        TEXT,
    nb_disponibles   INT,
    PRIMARY KEY ((auteur_nom, auteur_prenom), titre, livre_id)
) WITH CLUSTERING ORDER BY (titre ASC);
"""

# Table 3 : Emprunts actifs par membre (Q3)
CQL_TABLE_EMPRUNTS_PAR_MEMBRE = """
-- Q3 : "Quels sont les emprunts actifs du membre X ?"
CREATE TABLE IF NOT EXISTS emprunts_par_membre (
    membre_id        INT,
    membre_email     TEXT,
    date_emprunt     TEXT,
    emprunt_id       INT,
    livre_titre      TEXT,
    livre_isbn       TEXT,
    exemplaire_id    INT,
    date_retour_prevue TEXT,
    date_retour_reelle TEXT,
    statut           TEXT,
    PRIMARY KEY ((membre_id), date_emprunt, emprunt_id)
) WITH CLUSTERING ORDER BY (date_emprunt DESC);
"""

# Table 4 : Livres disponibles (Q4)
CQL_TABLE_DISPONIBILITE = """
-- Q4 : "Quels exemplaires du livre X sont disponibles ?"
CREATE TABLE IF NOT EXISTS disponibilite_livres (
    livre_id         INT,
    titre            TEXT,
    exemplaire_id    INT,
    etat             TEXT,
    disponible       BOOLEAN,
    PRIMARY KEY ((livre_id), exemplaire_id)
);
"""

# Table 5 : Emprunts en retard (Q5)
CQL_TABLE_RETARDS = """
-- Q5 : "Quels emprunts sont en retard ?"
CREATE TABLE IF NOT EXISTS emprunts_en_retard (
    statut           TEXT,
    date_retour_prevue TEXT,
    emprunt_id       INT,
    membre_nom       TEXT,
    membre_prenom    TEXT,
    membre_email     TEXT,
    livre_titre      TEXT,
    date_emprunt     TEXT,
    PRIMARY KEY ((statut), date_retour_prevue, emprunt_id)
) WITH CLUSTERING ORDER BY (date_retour_prevue ASC);
"""

# Table 6 : Membres par ville (Q6)
CQL_TABLE_MEMBRES_PAR_VILLE = """
-- Q6 : "Quels membres habitent dans la ville X ?"
CREATE TABLE IF NOT EXISTS membres_par_ville (
    ville            TEXT,
    nom              TEXT,
    prenom           TEXT,
    membre_id        INT,
    email            TEXT,
    telephone        TEXT,
    adresse          TEXT,
    date_inscription TEXT,
    PRIMARY KEY ((ville), nom, prenom, membre_id)
) WITH CLUSTERING ORDER BY (nom ASC, prenom ASC);
"""


# ══════════════════════════════════════════════════════════════════════════════
#  GÉNÉRATION DES INSERT CQL
# ══════════════════════════════════════════════════════════════════════════════

def fetch_all(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def build_cql_inserts():
    conn = sqlite3.connect(DB_PATH)

    categories  = {r["id"]: r for r in fetch_all(conn, "SELECT * FROM categories")}
    auteurs     = {r["id"]: r for r in fetch_all(conn, "SELECT * FROM auteurs")}
    livres_raw  = fetch_all(conn, "SELECT * FROM livres")
    membres     = fetch_all(conn, "SELECT * FROM membres")
    exemplaires = fetch_all(conn, "SELECT * FROM exemplaires")
    emprunts    = fetch_all(conn, "SELECT * FROM emprunts")

    inserts = []

    # ── livres_par_categorie & livres_par_auteur ─────────────────────────────
    for l in livres_raw:
        a   = auteurs[l["auteur_id"]]
        cat = categories[l["categorie_id"]]
        nb_ex   = sum(1 for e in exemplaires if e["livre_id"] == l["id"])
        nb_dispo = sum(1 for e in exemplaires if e["livre_id"] == l["id"] and e["disponible"])

        inserts.append(
            f"INSERT INTO livres_par_categorie "
            f"(categorie, titre, livre_id, isbn, annee_pub, editeur, prix, "
            f"auteur_nom, auteur_prenom, auteur_nationalite, nb_exemplaires, nb_disponibles) "
            f"VALUES ('{cat['nom']}', '{l['titre'].replace(chr(39),chr(39)*2)}', {l['id']}, "
            f"'{l['isbn']}', {l['annee_pub']}, '{l['editeur']}', {l['prix']}, "
            f"'{a['nom']}', '{a['prenom']}', '{a['nationalite']}', {nb_ex}, {nb_dispo});"
        )

        inserts.append(
            f"INSERT INTO livres_par_auteur "
            f"(auteur_nom, auteur_prenom, titre, livre_id, isbn, annee_pub, editeur, prix, "
            f"categorie, nb_disponibles) "
            f"VALUES ('{a['nom']}', '{a['prenom']}', '{l['titre'].replace(chr(39),chr(39)*2)}', "
            f"{l['id']}, '{l['isbn']}', {l['annee_pub']}, '{l['editeur']}', {l['prix']}, "
            f"'{cat['nom']}', {nb_dispo});"
        )

    # ── membres_par_ville ────────────────────────────────────────────────────
    for m in membres:
        inserts.append(
            f"INSERT INTO membres_par_ville "
            f"(ville, nom, prenom, membre_id, email, telephone, adresse, date_inscription) "
            f"VALUES ('{m['ville']}', '{m['nom']}', '{m['prenom']}', {m['id']}, "
            f"'{m['email']}', '{m['telephone']}', '{m['adresse']}', '{m['date_inscription']}');"
        )

    # ── disponibilite_livres ─────────────────────────────────────────────────
    for e in exemplaires:
        l = next(lv for lv in livres_raw if lv["id"] == e["livre_id"])
        dispo = "true" if e["disponible"] else "false"
        inserts.append(
            f"INSERT INTO disponibilite_livres "
            f"(livre_id, titre, exemplaire_id, etat, disponible) "
            f"VALUES ({l['id']}, '{l['titre'].replace(chr(39),chr(39)*2)}', "
            f"{e['id']}, '{e['etat']}', {dispo});"
        )

    # ── emprunts_par_membre & emprunts_en_retard ─────────────────────────────
    membres_dict = {m["id"]: m for m in membres}
    for em in emprunts:
        ex  = next(e for e in exemplaires if e["id"] == em["exemplaire_id"])
        l   = next(lv for lv in livres_raw if lv["id"] == ex["livre_id"])
        mem = membres_dict[em["membre_id"]]
        ret = f"'{em['date_retour_reelle']}'" if em["date_retour_reelle"] else "null"

        inserts.append(
            f"INSERT INTO emprunts_par_membre "
            f"(membre_id, membre_email, date_emprunt, emprunt_id, livre_titre, livre_isbn, "
            f"exemplaire_id, date_retour_prevue, date_retour_reelle, statut) "
            f"VALUES ({mem['id']}, '{mem['email']}', '{em['date_emprunt']}', {em['id']}, "
            f"'{l['titre'].replace(chr(39),chr(39)*2)}', '{l['isbn']}', {ex['id']}, "
            f"'{em['date_retour_prevue']}', {ret}, '{em['statut']}');"
        )

        if em["statut"] in ("retard", "en_cours"):
            inserts.append(
                f"INSERT INTO emprunts_en_retard "
                f"(statut, date_retour_prevue, emprunt_id, membre_nom, membre_prenom, "
                f"membre_email, livre_titre, date_emprunt) "
                f"VALUES ('{em['statut']}', '{em['date_retour_prevue']}', {em['id']}, "
                f"'{mem['nom']}', '{mem['prenom']}', '{mem['email']}', "
                f"'{l['titre'].replace(chr(39),chr(39)*2)}', '{em['date_emprunt']}');"
            )

    conn.close()
    return inserts


def simulate_cql_queries():
    """Simule les résultats des requêtes CQL"""
    conn = sqlite3.connect(DB_PATH)
    results = {}

    # Q1 : livres d'informatique
    cur = conn.cursor()
    cur.execute("""
        SELECT l.titre, a.nom, a.prenom
        FROM livres l JOIN auteurs a ON a.id=l.auteur_id
        JOIN categories c ON c.id=l.categorie_id
        WHERE c.nom='Informatique' ORDER BY l.titre
    """)
    results["Q1_livres_informatique"] = [{"titre":r[0],"auteur":f"{r[1]} {r[2]}"} for r in cur.fetchall()]

    # Q2 : livres de Fowler
    cur.execute("""
        SELECT l.titre, l.annee_pub, c.nom
        FROM livres l JOIN auteurs a ON a.id=l.auteur_id
        JOIN categories c ON c.id=l.categorie_id
        WHERE a.nom='Fowler' ORDER BY l.titre
    """)
    results["Q2_livres_auteur_Fowler"] = [{"titre":r[0],"annee":r[1],"categorie":r[2]} for r in cur.fetchall()]

    # Q3 : emprunts en cours
    cur.execute("""
        SELECT m.nom, m.prenom, l.titre, e.date_emprunt, e.date_retour_prevue
        FROM emprunts e
        JOIN membres m ON m.id=e.membre_id
        JOIN exemplaires ex ON ex.id=e.exemplaire_id
        JOIN livres l ON l.id=ex.livre_id
        WHERE e.statut='en_cours'
    """)
    results["Q3_emprunts_en_cours"] = [{"membre":f"{r[0]} {r[1]}","livre":r[2],"depuis":r[3]} for r in cur.fetchall()]

    # Q4 : membres par ville Tunis
    cur.execute("SELECT nom, prenom, email FROM membres WHERE ville='Tunis' ORDER BY nom")
    results["Q4_membres_Tunis"] = [{"nom":r[0],"prenom":r[1],"email":r[2]} for r in cur.fetchall()]

    # Q5 : retards
    cur.execute("""
        SELECT m.nom, m.prenom, l.titre, e.date_retour_prevue
        FROM emprunts e
        JOIN membres m ON m.id=e.membre_id
        JOIN exemplaires ex ON ex.id=e.exemplaire_id
        JOIN livres l ON l.id=ex.livre_id
        WHERE e.statut='retard'
    """)
    results["Q5_retards"] = [{"membre":f"{r[0]} {r[1]}","livre":r[2],"prevue":r[3]} for r in cur.fetchall()]

    conn.close()
    return results


if __name__ == "__main__":
    inserts = build_cql_inserts()
    results = simulate_cql_queries()

    full_cql = "\n".join([
        "-- ════════════════════════════════════════════════════════",
        "-- SCHÉMA CASSANDRA — Bibliothèque",
        "-- ════════════════════════════════════════════════════════",
        CQL_KEYSPACE,
        CQL_TABLE_LIVRES_PAR_CATEGORIE,
        CQL_TABLE_LIVRES_PAR_AUTEUR,
        CQL_TABLE_EMPRUNTS_PAR_MEMBRE,
        CQL_TABLE_DISPONIBILITE,
        CQL_TABLE_RETARDS,
        CQL_TABLE_MEMBRES_PAR_VILLE,
        "\n-- ════════ INSERT ════════\n",
    ] + inserts)

    with open("schema_cassandra.cql", "w", encoding="utf-8") as f:
        f.write(full_cql)

    with open("cassandra_queries.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"[OK] {len(inserts)} instructions CQL générées → schema_cassandra.cql")
    print(f"[OK] Résultats des requêtes → cassandra_queries.json")

    print("\n── Résultats des requêtes Cassandra ──")
    for k, v in results.items():
        print(f"\n  {k} ({len(v)} résultats):")
        for item in v:
            print(f"    {item}")
"""
06_requetes_avancees_cassandra.py
Requêtes Cassandra Avancées — Niveau Expert
Couvre : Partition avancée, TTL, Counters, Materialized Views,
         SASI Index, Batch, Lightweight Transactions (LWT),
         UDA (User Defined Aggregates), Time-series, Tombstones
"""
import sqlite3, json
from collections import defaultdict, Counter
from datetime import datetime

DB_PATH = "bibliotheque.db"

def fetch(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def load_data():
    conn = sqlite3.connect(DB_PATH)
    d = {
        "categories":  {r["id"]: r for r in fetch(conn,"SELECT * FROM categories")},
        "auteurs":     {r["id"]: r for r in fetch(conn,"SELECT * FROM auteurs")},
        "livres":      fetch(conn,"SELECT * FROM livres"),
        "membres":     fetch(conn,"SELECT * FROM membres"),
        "exemplaires": fetch(conn,"SELECT * FROM exemplaires"),
        "emprunts":    fetch(conn,"SELECT * FROM emprunts"),
    }
    conn.close()
    return d


# ════════════════════════════════════════════════════════════════════════════
# SCHÉMAS AVANCÉS CQL
# ════════════════════════════════════════════════════════════════════════════

SCHEMAS_AVANCES = {

"C1_compteurs": """
-- ─────────────────────────────────────────────────────────────────────────
-- C1 : COUNTER TABLE — Statistiques d'emprunts en temps réel
-- Les COUNTER ne peuvent contenir que des colonnes counter
-- Mise à jour atomique, pas de lecture avant écriture
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS stats_emprunts_compteurs (
    livre_id      INT,
    PRIMARY KEY   (livre_id)
) WITH comment = 'Compteurs atomiques pour statistiques live';

-- En réalité avec des COUNTER :
CREATE TABLE IF NOT EXISTS compteurs_livres (
    livre_id           INT PRIMARY KEY,
    nb_emprunts_total  COUNTER,
    nb_emprunts_mois   COUNTER,
    nb_consultations   COUNTER
);

-- Incrémenter (opération atomique distribuée) :
-- UPDATE compteurs_livres SET nb_emprunts_total = nb_emprunts_total + 1
-- WHERE livre_id = 2;
""",

"C2_ttl": """
-- ─────────────────────────────────────────────────────────────────────────
-- C2 : TTL (Time To Live) — Sessions et cache temporaire
-- Les données expirent automatiquement → pas de DELETE manuel
-- Idéal pour les tokens de session, cache de disponibilité
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sessions_membres (
    membre_id     INT,
    session_token TEXT,
    created_at    TIMESTAMP,
    ip_address    TEXT,
    user_agent    TEXT,
    PRIMARY KEY   ((membre_id), session_token)
);

-- Insertion avec TTL de 24h (86400 secondes) :
-- INSERT INTO sessions_membres (membre_id, session_token, created_at, ip_address)
-- VALUES (1, 'abc123xyz', toTimestamp(now()), '192.168.1.10')
-- USING TTL 86400;

-- Vérifier le TTL restant :
-- SELECT TTL(ip_address) FROM sessions_membres WHERE membre_id = 1;
""",

"C3_lwt": """
-- ─────────────────────────────────────────────────────────────────────────
-- C3 : LWT (Lightweight Transactions) — Transactions conditionnelles
-- Utilise Paxos pour garantir la cohérence
-- Parfait pour les réservations et éviter les doublons
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS reservations (
    exemplaire_id  INT,
    membre_id      INT,
    date_reservation TIMESTAMP,
    statut         TEXT,
    PRIMARY KEY    (exemplaire_id)
);

-- Réserver seulement si non réservé (LWT = IF NOT EXISTS) :
-- INSERT INTO reservations (exemplaire_id, membre_id, date_reservation, statut)
-- VALUES (5, 3, toTimestamp(now()), 'en_attente')
-- IF NOT EXISTS;
-- → Retourne [applied: true] ou [applied: false, <données existantes>]

-- Annuler la réservation seulement si elle appartient au membre :
-- DELETE FROM reservations
-- WHERE exemplaire_id = 5
-- IF membre_id = 3;
""",

"C4_materialized_view": """
-- ─────────────────────────────────────────────────────────────────────────
-- C4 : MATERIALIZED VIEWS — Vue automatiquement synchronisée
-- Evite de dupliquer manuellement les données
-- Cassandra maintient la vue à jour lors des écritures
-- ─────────────────────────────────────────────────────────────────────────

-- Table de base
CREATE TABLE IF NOT EXISTS livres_base (
    livre_id      INT,
    categorie     TEXT,
    titre         TEXT,
    isbn          TEXT,
    prix          DECIMAL,
    annee_pub     INT,
    PRIMARY KEY   (livre_id)
);

-- Vue matérialisée : accès par catégorie ET par prix
CREATE MATERIALIZED VIEW IF NOT EXISTS livres_par_categorie_prix AS
    SELECT * FROM livres_base
    WHERE categorie IS NOT NULL
      AND prix IS NOT NULL
      AND livre_id IS NOT NULL
PRIMARY KEY ((categorie), prix, livre_id)
WITH CLUSTERING ORDER BY (prix ASC);

-- Requête sur la vue :
-- SELECT titre, prix FROM livres_par_categorie_prix
-- WHERE categorie = 'Informatique'
-- AND prix <= 50;
""",

"C5_sasi_index": """
-- ─────────────────────────────────────────────────────────────────────────
-- C5 : SASI INDEX — Recherche textuelle avancée
-- Storage-Attached Secondary Index
-- Permet LIKE, range queries sur colonnes non-primaires
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS livres_recherche (
    livre_id   INT PRIMARY KEY,
    titre      TEXT,
    editeur    TEXT,
    annee_pub  INT,
    prix       DECIMAL
);

-- Index SASI sur titre (recherche par préfixe/sous-chaîne)
CREATE CUSTOM INDEX IF NOT EXISTS idx_titre_sasi
ON livres_recherche (titre)
USING 'org.apache.cassandra.index.sasi.SASIIndex'
WITH OPTIONS = {
    'mode': 'CONTAINS',
    'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer',
    'case_sensitive': 'false'
};

-- Requêtes SASI :
-- SELECT * FROM livres_recherche WHERE titre LIKE '%Code%';
-- SELECT * FROM livres_recherche WHERE titre LIKE 'Clean%';
-- SELECT * FROM livres_recherche WHERE annee_pub >= 2000 AND annee_pub <= 2010;
""",

"C6_batch": """
-- ─────────────────────────────────────────────────────────────────────────
-- C6 : BATCH — Écriture atomique multi-tables
-- Garantit que toutes les tables sont mises à jour
-- LOGGED BATCH = atomique | UNLOGGED = performance
-- ─────────────────────────────────────────────────────────────────────────

-- Lors d'un emprunt, mettre à jour 3 tables en une seule opération :
BEGIN BATCH
    -- 1. Ajouter l'emprunt dans la table des emprunts par membre
    INSERT INTO emprunts_par_membre
        (membre_id, date_emprunt, emprunt_id, livre_titre, statut)
    VALUES (3, '2024-04-01', 9, 'Clean Code', 'en_cours');

    -- 2. Marquer l'exemplaire comme non disponible
    UPDATE disponibilite_livres
    SET disponible = false
    WHERE livre_id = 2 AND exemplaire_id = 4;

    -- 3. Incrémenter le compteur (dans une table séparée)
    UPDATE compteurs_livres
    SET nb_emprunts_total = nb_emprunts_total + 1
    WHERE livre_id = 2;
APPLY BATCH;
""",

"C7_time_series": """
-- ─────────────────────────────────────────────────────────────────────────
-- C7 : TIME SERIES — Historique des événements
-- Cassandra excelle dans le stockage de données temporelles
-- Partition par (mois, livre_id) pour éviter les partitions énormes
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS historique_activite (
    annee_mois    TEXT,       -- '2024-03' = bucket mensuel
    livre_id      INT,
    event_time    TIMESTAMP,
    event_type    TEXT,       -- 'emprunt', 'retour', 'reservation'
    membre_id     INT,
    details       TEXT,
    PRIMARY KEY   ((annee_mois, livre_id), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC)
  AND default_time_to_live = 31536000;  -- TTL 1 an automatique

-- Requête : derniers événements du livre 2 en mars 2024
-- SELECT event_time, event_type, membre_id
-- FROM historique_activite
-- WHERE annee_mois = '2024-03' AND livre_id = 2
-- LIMIT 10;

-- Requête : activité des 7 derniers jours
-- SELECT * FROM historique_activite
-- WHERE annee_mois = '2024-03' AND livre_id = 2
-- AND event_time >= '2024-03-25 00:00:00'
-- AND event_time <= '2024-03-31 23:59:59';
""",

"C8_uda": """
-- ─────────────────────────────────────────────────────────────────────────
-- C8 : UDA (User Defined Aggregates) — Agrégats personnalisés
-- Permet de créer des fonctions d'agrégation custom
-- Exemple : calculer la médiane du prix des livres
-- ─────────────────────────────────────────────────────────────────────────

-- Fonction d'état (sfunc) : ajoute une valeur à la liste
CREATE OR REPLACE FUNCTION state_group_and_count(
    state MAP<TEXT, INT>,
    val TEXT
) CALLED ON NULL INPUT
RETURNS MAP<TEXT, INT>
LANGUAGE java AS
'
    if (val != null) {
        Integer count = state.get(val);
        if (count == null) count = 0;
        state.put(val, count + 1);
    }
    return state;
';

-- Agrégat : compte les occurrences de chaque valeur
CREATE AGGREGATE IF NOT EXISTS count_by_value(TEXT)
SFUNC state_group_and_count
STYPE MAP<TEXT, INT>
INITCOND {};

-- Utilisation :
-- SELECT count_by_value(statut) FROM emprunts_par_membre WHERE membre_id = 1;
-- → {'en_cours': 2, 'rendu': 1, 'retard': 1}
""",

"C9_partition_avancee": """
-- ─────────────────────────────────────────────────────────────────────────
-- C9 : PARTITIONNEMENT AVANCÉ — Éviter les hot partitions
-- Une partition trop grande = problème de performance
-- Stratégie : bucketing par période + par ID
-- ─────────────────────────────────────────────────────────────────────────

-- ❌ MAUVAIS : partition key = ville → une seule ville peut avoir
--    des millions de membres → hot partition
CREATE TABLE membres_par_ville_mauvais (
    ville     TEXT,
    membre_id INT,
    nom       TEXT,
    PRIMARY KEY (ville, membre_id)
);

-- ✅ BON : partition key composite (ville, bucket)
-- Distribue les données uniformément
CREATE TABLE IF NOT EXISTS membres_par_ville_bucket (
    ville     TEXT,
    bucket    INT,    -- = membre_id % 10 (0 à 9)
    membre_id INT,
    nom       TEXT,
    prenom    TEXT,
    email     TEXT,
    PRIMARY KEY ((ville, bucket), membre_id)
);

-- Requête nécessite de parcourir les 10 buckets :
-- SELECT * FROM membres_par_ville_bucket
-- WHERE ville = 'Tunis' AND bucket IN (0,1,2,3,4,5,6,7,8,9);
""",

"C10_consistency_levels": """
-- ─────────────────────────────────────────────────────────────────────────
-- C10 : NIVEAUX DE COHÉRENCE — CAP Theorem en pratique
-- Cassandra permet de choisir le niveau de cohérence par requête
-- Formule : R + W > RF → cohérence forte (quorum)
-- ─────────────────────────────────────────────────────────────────────────

-- Avec RF=3 (replication_factor=3) :

-- LECTURE haute disponibilité (1 réplique suffit)
CONSISTENCY ONE;
SELECT * FROM livres_par_categorie WHERE categorie = 'Informatique';

-- LECTURE cohérente (majorité des répliques : 2/3)
CONSISTENCY QUORUM;
SELECT * FROM emprunts_par_membre WHERE membre_id = 1;

-- ÉCRITURE cohérente (majorité : 2/3)
CONSISTENCY QUORUM;
INSERT INTO emprunts_par_membre (membre_id, date_emprunt, emprunt_id, statut)
VALUES (1, '2024-04-01', 10, 'en_cours');

-- COHÉRENCE FORTE (toutes les répliques doivent confirmer)
CONSISTENCY ALL;
UPDATE disponibilite_livres SET disponible = false
WHERE livre_id = 1 AND exemplaire_id = 1;

-- LOCAL_QUORUM : quorum dans le datacenter local seulement
-- Utilisé pour les déploiements multi-datacenter
CONSISTENCY LOCAL_QUORUM;
SELECT * FROM membres_par_ville WHERE ville = 'Tunis';
"""
}


def simulate_advanced_queries(data):
    """Simule les résultats des requêtes avancées Cassandra"""
    livres     = data["livres"]
    auteurs    = data["auteurs"]
    categories = data["categories"]
    membres    = data["membres"]
    exemplaires= data["exemplaires"]
    emprunts   = data["emprunts"]

    results = {}

    # C1 — Simulation compteurs
    compteurs = {}
    for l in livres:
        nb_emp = sum(1 for em in emprunts
                     for e in exemplaires
                     if e["livre_id"]==l["id"] and em["exemplaire_id"]==e["id"])
        compteurs[l["titre"]] = {
            "livre_id":          l["id"],
            "nb_emprunts_total": nb_emp,
            "nb_consultations":  nb_emp * 3,  # simulé
        }
    results["C1_compteurs_simulation"] = sorted(
        [{"titre":k,**v} for k,v in compteurs.items()],
        key=lambda x: -x["nb_emprunts_total"]
    )

    # C3 — Simulation LWT : réservations potentielles
    exs_non_dispo = [e for e in exemplaires if not e["disponible"]]
    lwt_sim = []
    for e in exs_non_dispo:
        l = next((lv for lv in livres if lv["id"]==e["livre_id"]), None)
        em = next((em for em in emprunts if em["exemplaire_id"]==e["id"] and em["statut"]!="rendu"), None)
        if l and em:
            lwt_sim.append({
                "exemplaire_id": e["id"],
                "livre":         l["titre"],
                "statut_lwt":    "INSERT IF NOT EXISTS → [applied: false] (déjà emprunté)",
                "date_dispo_estimee": em["date_retour_prevue"]
            })
    results["C3_lwt_reservations"] = lwt_sim

    # C7 — Time series : historique des événements
    events = []
    for em in emprunts:
        ex = next((e for e in exemplaires if e["id"]==em["exemplaire_id"]), None)
        l  = next((lv for lv in livres if lv["id"]==ex["livre_id"]), None) if ex else None
        if l:
            mois = em["date_emprunt"][:7]
            events.append({
                "annee_mois":  mois,
                "livre_id":    l["id"],
                "livre_titre": l["titre"],
                "event_time":  em["date_emprunt"],
                "event_type":  "emprunt",
                "membre_id":   em["membre_id"]
            })
            if em["date_retour_reelle"]:
                events.append({
                    "annee_mois":  em["date_retour_reelle"][:7],
                    "livre_id":    l["id"],
                    "livre_titre": l["titre"],
                    "event_time":  em["date_retour_reelle"],
                    "event_type":  "retour",
                    "membre_id":   em["membre_id"]
                })
    results["C7_time_series_events"] = sorted(events, key=lambda x: x["event_time"])

    # C9 — Bucketing simulation
    bucket_data = []
    for m in membres:
        bucket = m["id"] % 4  # 4 buckets
        bucket_data.append({
            "ville":     m["ville"],
            "bucket":    bucket,
            "membre_id": m["id"],
            "nom":       m["nom"],
            "prenom":    m["prenom"]
        })
    results["C9_bucketing_membres"] = sorted(bucket_data, key=lambda x: (x["ville"], x["bucket"]))

    # Analyse de la taille des partitions
    partition_sizes = Counter((m["ville"]) for m in membres)
    results["C9_analyse_partitions"] = {
        "avant_bucketing": dict(partition_sizes),
        "apres_bucketing": {
            f"{ville}_bucket_{b}": sum(1 for m in membres if m["ville"]==ville and m["id"]%4==b)
            for ville in set(m["ville"] for m in membres)
            for b in range(4)
            if any(m["ville"]==ville and m["id"]%4==b for m in membres)
        }
    }

    # C10 — Recommandations de consistency level
    results["C10_consistency_recommendations"] = [
        {"operation": "Recherche catalogue livres",   "consistency": "ONE",          "raison": "Lecture non critique, haute dispo"},
        {"operation": "Consultation disponibilité",   "consistency": "LOCAL_QUORUM", "raison": "Données fréquemment modifiées"},
        {"operation": "Enregistrement emprunt",       "consistency": "QUORUM",       "raison": "Écriture importante, cohérence nécessaire"},
        {"operation": "Mise à jour disponibilité",    "consistency": "QUORUM",       "raison": "Éviter double réservation"},
        {"operation": "Création compte membre",       "consistency": "ALL",          "raison": "Unicité email obligatoire"},
        {"operation": "Lecture statistiques/rapports","consistency": "ONE",          "raison": "Légère inconsistance acceptable"},
    ]

    return results


def generate_full_cql_advanced():
    """Génère un fichier CQL avec tous les schémas avancés"""
    lines = [
        "-- ════════════════════════════════════════════════════════════════",
        "-- REQUÊTES CASSANDRA AVANCÉES — Bibliothèque",
        "-- Concepts : Counter, TTL, LWT, Materialized Views, SASI,",
        "-- Batch, Time-Series, UDA, Partitionnement, Consistency Levels",
        "-- ════════════════════════════════════════════════════════════════",
        "",
        "USE bibliotheque;",
        ""
    ]
    for name, cql in SCHEMAS_AVANCES.items():
        lines.append(f"\n-- {'─'*60}")
        lines.append(f"-- {name}")
        lines.append(f"-- {'─'*60}")
        lines.append(cql)

    return "\n".join(lines)


if __name__ == "__main__":
    data    = load_data()
    results = simulate_advanced_queries(data)
    cql_adv = generate_full_cql_advanced()

    with open("schema_cassandra_avance.cql","w",encoding="utf-8") as f:
        f.write(cql_adv)

    with open("cassandra_requetes_avancees.json","w",encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print("═"*70)
    print("   REQUÊTES AVANCÉES CASSANDRA — RÉSULTATS")
    print("═"*70)

    print("\n── C1 : Compteurs atomiques (simulation) ──")
    for item in results["C1_compteurs_simulation"]:
        print(f"  {item['titre'][:40]:40s} | emprunts: {item['nb_emprunts_total']} | consultations: {item['nb_consultations']}")

    print("\n── C3 : LWT — Réservations conditionnelles ──")
    for item in results["C3_lwt_reservations"]:
        print(f"  {item['livre'][:35]:35s} | {item['statut_lwt'][:40]}")

    print("\n── C7 : Time Series — Événements ──")
    for item in results["C7_time_series_events"]:
        print(f"  {item['event_time']} | {item['event_type']:8s} | livre_id={item['livre_id']} | membre={item['membre_id']}")

    print("\n── C9 : Analyse des partitions ──")
    print("  Avant bucketing :", results["C9_analyse_partitions"]["avant_bucketing"])
    print("  Après bucketing  (4 buckets par ville) :")
    for k,v in results["C9_analyse_partitions"]["apres_bucketing"].items():
        if v > 0: print(f"    {k}: {v}")

    print("\n── C10 : Recommandations Consistency Level ──")
    for r in results["C10_consistency_recommendations"]:
        print(f"  {r['operation']:35s} → {r['consistency']:15s} ({r['raison']})")

    print(f"\n[OK] Schémas CQL avancés → schema_cassandra_avance.cql")
    print(f"[OK] Résultats JSON       → cassandra_requetes_avancees.json")
