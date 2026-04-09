-- ════════════════════════════════════════════════════════════════
-- REQUÊTES CASSANDRA AVANCÉES — Bibliothèque
-- Concepts : Counter, TTL, LWT, Materialized Views, SASI,
-- Batch, Time-Series, UDA, Partitionnement, Consistency Levels
-- ════════════════════════════════════════════════════════════════

USE bibliotheque;


-- ────────────────────────────────────────────────────────────
-- C1_compteurs
-- ────────────────────────────────────────────────────────────

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


-- ────────────────────────────────────────────────────────────
-- C2_ttl
-- ────────────────────────────────────────────────────────────

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


-- ────────────────────────────────────────────────────────────
-- C3_lwt
-- ────────────────────────────────────────────────────────────

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


-- ────────────────────────────────────────────────────────────
-- C4_materialized_view
-- ────────────────────────────────────────────────────────────

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


-- ────────────────────────────────────────────────────────────
-- C5_sasi_index
-- ────────────────────────────────────────────────────────────

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


-- ────────────────────────────────────────────────────────────
-- C6_batch
-- ────────────────────────────────────────────────────────────

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


-- ────────────────────────────────────────────────────────────
-- C7_time_series
-- ────────────────────────────────────────────────────────────

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


-- ────────────────────────────────────────────────────────────
-- C8_uda
-- ────────────────────────────────────────────────────────────

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


-- ────────────────────────────────────────────────────────────
-- C9_partition_avancee
-- ────────────────────────────────────────────────────────────

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


-- ────────────────────────────────────────────────────────────
-- C10_consistency_levels
-- ────────────────────────────────────────────────────────────

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
