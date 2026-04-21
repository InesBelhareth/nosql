"""
04_migration_neo4j.py
Migration vers Neo4j — Base de Données Graphe
Stratégie : modélisation orientée relations (Graph-Driven Design)

Structure du graphe :
  Nœuds    : Livre, Auteur, Membre, Categorie, Exemplaire
  Relations : A_ECRIT, APPARTIENT_A, A_EXEMPLAIRE,
              A_EMPRUNTE, A_RENDU, EN_RETARD, RECOMMANDE
"""

import sqlite3
import json

DB_PATH = "bibliotheque.db"

# ══════════════════════════════════════════════════════════════════════════════
#  SCHÉMA CYPHER — Contraintes et Index
# ══════════════════════════════════════════════════════════════════════════════

CYPHER_CONSTRAINTS = """
// ─────────────────────────────────────────────────────────────────────────
// CONTRAINTES D'UNICITÉ ET INDEX
// Garantissent l'intégrité et accélèrent les recherches
// ─────────────────────────────────────────────────────────────────────────

// Contraintes d'unicité (crée aussi un index)
CREATE CONSTRAINT IF NOT EXISTS FOR (l:Livre)      REQUIRE l.livre_id    IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (a:Auteur)     REQUIRE a.auteur_id   IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (m:Membre)     REQUIRE m.membre_id   IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (c:Categorie)  REQUIRE c.nom         IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (e:Exemplaire) REQUIRE e.exemplaire_id IS UNIQUE;

// Index sur propriétés fréquemment recherchées
CREATE INDEX IF NOT EXISTS FOR (l:Livre)   ON (l.titre);
CREATE INDEX IF NOT EXISTS FOR (l:Livre)   ON (l.categorie_nom);
CREATE INDEX IF NOT EXISTS FOR (m:Membre)  ON (m.ville);
CREATE INDEX IF NOT EXISTS FOR (m:Membre)  ON (m.email);
"""

# ══════════════════════════════════════════════════════════════════════════════
#  REQUÊTES CYPHER AVANCÉES (modèle graphe)
# ══════════════════════════════════════════════════════════════════════════════

CYPHER_QUERIES = {

"G1_livres_par_categorie": """
// G1 : Livres d'une catégorie (traversée de relation)
MATCH (l:Livre)-[:APPARTIENT_A]->(c:Categorie {nom: 'Informatique'})
MATCH (a:Auteur)-[:A_ECRIT]->(l)
RETURN l.titre AS titre,
       a.nom + ' ' + a.prenom AS auteur,
       l.prix AS prix
ORDER BY l.titre;
""",

"G2_livres_auteur": """
// G2 : Livres d'un auteur avec sa catégorie
MATCH (a:Auteur {nom: 'Fowler'})-[:A_ECRIT]->(l:Livre)-[:APPARTIENT_A]->(c:Categorie)
RETURN l.titre AS titre, l.annee_pub AS annee, c.nom AS categorie
ORDER BY l.annee_pub DESC;
""",

"G3_emprunts_en_cours": """
// G3 : Tous les emprunts actifs (chemin membre → exemplaire → livre)
MATCH (m:Membre)-[e:A_EMPRUNTE]->(ex:Exemplaire)<-[:A_EXEMPLAIRE]-(l:Livre)
WHERE e.statut IN ['en_cours', 'retard']
RETURN m.prenom + ' ' + m.nom AS membre,
       l.titre AS livre,
       e.date_emprunt AS depuis,
       e.statut AS statut
ORDER BY e.date_emprunt;
""",

"G4_membres_ville": """
// G4 : Membres d'une ville
MATCH (m:Membre {ville: 'Tunis'})
RETURN m.nom AS nom, m.prenom AS prenom, m.email AS email
ORDER BY m.nom;
""",

"G5_retards": """
// G5 : Emprunts en retard avec calcul du chemin complet
MATCH (m:Membre)-[e:A_EMPRUNTE]->(ex:Exemplaire)<-[:A_EXEMPLAIRE]-(l:Livre)
WHERE e.statut = 'retard'
RETURN m.prenom + ' ' + m.nom AS membre,
       l.titre AS livre,
       e.date_retour_prevue AS prevue,
       m.email AS contact
ORDER BY e.date_retour_prevue;
""",

"G6_recommandations": """
// G6 : Recommandations collaboratives — "Les lecteurs qui ont lu X ont aussi lu Y"
// Membres qui partagent des lectures communes
MATCH (m1:Membre)-[:A_EMPRUNTE]->(:Exemplaire)<-[:A_EXEMPLAIRE]-(l1:Livre),
      (m1)-[:A_EMPRUNTE]->(:Exemplaire)<-[:A_EXEMPLAIRE]-(l2:Livre)
WHERE l1 <> l2
  AND NOT (m1)-[:A_EMPRUNTE]->(:Exemplaire)<-[:A_EXEMPLAIRE]-(l2)
RETURN DISTINCT l1.titre AS livre_source,
                l2.titre AS recommandation,
                count(DISTINCT m1) AS lecteurs_communs
ORDER BY lecteurs_communs DESC
LIMIT 10;
""",

"G7_chemin_plus_court": """
// G7 : Chemin le plus court entre deux membres (via livres communs)
// Utilise l'algorithme de plus court chemin de Neo4j
MATCH path = shortestPath(
    (m1:Membre {nom: 'Ben Ali'})-[*..6]-(m2:Membre {nom: 'Chaabane'})
)
RETURN [node IN nodes(path) | coalesce(node.titre, node.nom, node.nom)] AS chemin,
       length(path) AS distance;
""",

"G8_centralite_livres": """
// G8 : Centralité des livres (degré de connexion = popularité)
MATCH (l:Livre)<-[:A_EXEMPLAIRE]-(ex:Exemplaire)
OPTIONAL MATCH (ex)<-[e:A_EMPRUNTE]-(:Membre)
WITH l,
     count(DISTINCT ex) AS nb_exemplaires,
     count(DISTINCT e)  AS nb_emprunts
RETURN l.titre AS titre,
       nb_exemplaires,
       nb_emprunts,
       nb_emprunts * 1.0 / nb_exemplaires AS taux_utilisation
ORDER BY nb_emprunts DESC;
""",

"G9_membres_connectes": """
// G9 : Réseau de lecture — membres connectés par livres communs
MATCH (m1:Membre)-[:A_EMPRUNTE]->(:Exemplaire)<-[:A_EXEMPLAIRE]-(l:Livre)
      <-[:A_EXEMPLAIRE]-(:Exemplaire)<-[:A_EMPRUNTE]-(m2:Membre)
WHERE m1 <> m2 AND id(m1) < id(m2)
RETURN m1.prenom + ' ' + m1.nom AS membre1,
       m2.prenom + ' ' + m2.nom AS membre2,
       collect(DISTINCT l.titre) AS livres_communs,
       count(DISTINCT l) AS nb_livres_communs
ORDER BY nb_livres_communs DESC;
""",

"G10_analyse_categories": """
// G10 : Statistiques par catégorie (aggregation graphe)
MATCH (c:Categorie)<-[:APPARTIENT_A]-(l:Livre)<-[:A_EXEMPLAIRE]-(ex:Exemplaire)
OPTIONAL MATCH (ex)<-[e:A_EMPRUNTE]-(:Membre)
WITH c.nom AS categorie,
     count(DISTINCT l)  AS nb_livres,
     count(DISTINCT ex) AS nb_exemplaires,
     count(DISTINCT e)  AS nb_emprunts,
     avg(l.prix) AS prix_moyen,
     sum(l.prix * 2) AS valeur_stock
RETURN categorie, nb_livres, nb_exemplaires,
       nb_emprunts, round(prix_moyen, 2) AS prix_moyen,
       valeur_stock
ORDER BY nb_livres DESC;
"""
}

# ══════════════════════════════════════════════════════════════════════════════
#  CHARGEMENT DES DONNÉES SQLITE
# ══════════════════════════════════════════════════════════════════════════════

def fetch_all(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def load_data():
    conn = sqlite3.connect(DB_PATH)
    data = {
        "categories":  fetch_all(conn, "SELECT * FROM categories"),
        "auteurs":     fetch_all(conn, "SELECT * FROM auteurs"),
        "livres":      fetch_all(conn, "SELECT * FROM livres"),
        "membres":     fetch_all(conn, "SELECT * FROM membres"),
        "exemplaires": fetch_all(conn, "SELECT * FROM exemplaires"),
        "emprunts":    fetch_all(conn, "SELECT * FROM emprunts"),
    }
    conn.close()
    return data


# ══════════════════════════════════════════════════════════════════════════════
#  GÉNÉRATION DES INSTRUCTIONS CYPHER (CREATE/MERGE)
# ══════════════════════════════════════════════════════════════════════════════

def build_cypher_nodes(data):
    """Génère les instructions Cypher pour créer tous les nœuds"""
    lines = []

    # ── Nœuds Categorie ──────────────────────────────────────────────────────
    lines.append("\n// ─── Nœuds Categorie ─────────────────────────────────────")
    for c in data["categories"]:
        desc = c["description"].replace("'", "\\'") if c["description"] else ""
        lines.append(
            f"MERGE (:Categorie {{nom: '{c['nom']}', "
            f"description: '{desc}'}});"
        )

    # ── Nœuds Auteur ─────────────────────────────────────────────────────────
    lines.append("\n// ─── Nœuds Auteur ────────────────────────────────────────")
    for a in data["auteurs"]:
        nationalite = a["nationalite"].replace("'", "\\'") if a["nationalite"] else ""
        lines.append(
            f"MERGE (:Auteur {{auteur_id: {a['id']}, "
            f"nom: '{a['nom']}', prenom: '{a['prenom']}', "
            f"nationalite: '{nationalite}', "
            f"date_naissance: '{a['date_naissance']}'}});"
        )

    # ── Nœuds Livre ──────────────────────────────────────────────────────────
    lines.append("\n// ─── Nœuds Livre ─────────────────────────────────────────")
    for l in data["livres"]:
        titre   = l["titre"].replace("'", "\\'")
        editeur = l["editeur"].replace("'", "\\'") if l["editeur"] else ""
        cat_nom = next(c["nom"] for c in data["categories"] if c["id"] == l["categorie_id"])
        lines.append(
            f"MERGE (:Livre {{livre_id: {l['id']}, "
            f"titre: '{titre}', "
            f"isbn: '{l['isbn']}', "
            f"annee_pub: {l['annee_pub']}, "
            f"editeur: '{editeur}', "
            f"prix: {l['prix']}, "
            f"categorie_nom: '{cat_nom}'}});"
        )

    # ── Nœuds Membre ─────────────────────────────────────────────────────────
    lines.append("\n// ─── Nœuds Membre ────────────────────────────────────────")
    for m in data["membres"]:
        adresse = m["adresse"].replace("'", "\\'") if m["adresse"] else ""
        lines.append(
            f"MERGE (:Membre {{membre_id: {m['id']}, "
            f"nom: '{m['nom']}', prenom: '{m['prenom']}', "
            f"email: '{m['email']}', "
            f"telephone: '{m['telephone']}', "
            f"adresse: '{adresse}', "
            f"ville: '{m['ville']}', "
            f"date_inscription: '{m['date_inscription']}'}});"
        )

    # ── Nœuds Exemplaire ─────────────────────────────────────────────────────
    lines.append("\n// ─── Nœuds Exemplaire ────────────────────────────────────")
    for e in data["exemplaires"]:
        dispo = "true" if e["disponible"] else "false"
        lines.append(
            f"MERGE (:Exemplaire {{exemplaire_id: {e['id']}, "
            f"etat: '{e['etat']}', "
            f"disponible: {dispo}}});"
        )

    return "\n".join(lines)


def build_cypher_relations(data):
    """Génère les instructions Cypher pour créer toutes les relations"""
    lines = []

    # ── (Auteur)-[:A_ECRIT]->(Livre) ─────────────────────────────────────────
    lines.append("\n// ─── Relations A_ECRIT ───────────────────────────────────")
    for l in data["livres"]:
        lines.append(
            f"MATCH (a:Auteur {{auteur_id: {l['auteur_id']}}}), "
            f"(lv:Livre {{livre_id: {l['id']}}})\n"
            f"MERGE (a)-[:A_ECRIT]->(lv);"
        )

    # ── (Livre)-[:APPARTIENT_A]->(Categorie) ─────────────────────────────────
    lines.append("\n// ─── Relations APPARTIENT_A ──────────────────────────────")
    for l in data["livres"]:
        cat = next(c["nom"] for c in data["categories"] if c["id"] == l["categorie_id"])
        titre = l["titre"].replace("'", "\\'")
        lines.append(
            f"MATCH (lv:Livre {{livre_id: {l['id']}}}), "
            f"(c:Categorie {{nom: '{cat}'}})\n"
            f"MERGE (lv)-[:APPARTIENT_A]->(c);"
        )

    # ── (Livre)-[:A_EXEMPLAIRE]->(Exemplaire) ────────────────────────────────
    lines.append("\n// ─── Relations A_EXEMPLAIRE ──────────────────────────────")
    for e in data["exemplaires"]:
        lines.append(
            f"MATCH (lv:Livre {{livre_id: {e['livre_id']}}}), "
            f"(ex:Exemplaire {{exemplaire_id: {e['id']}}})\n"
            f"MERGE (lv)-[:A_EXEMPLAIRE]->(ex);"
        )

    # ── (Membre)-[:A_EMPRUNTE {props}]->(Exemplaire) ─────────────────────────
    lines.append("\n// ─── Relations A_EMPRUNTE (avec propriétés) ──────────────")
    for em in data["emprunts"]:
        date_ret = f"'{em['date_retour_reelle']}'" if em["date_retour_reelle"] else "null"
        rel_type = {
            "en_cours": "A_EMPRUNTE",
            "rendu":    "A_EMPRUNTE",
            "retard":   "A_EMPRUNTE",
        }[em["statut"]]
        lines.append(
            f"MATCH (m:Membre {{membre_id: {em['membre_id']}}}), "
            f"(ex:Exemplaire {{exemplaire_id: {em['exemplaire_id']}}})\n"
            f"MERGE (m)-[:{rel_type} {{"
            f"emprunt_id: {em['id']}, "
            f"date_emprunt: '{em['date_emprunt']}', "
            f"date_retour_prevue: '{em['date_retour_prevue']}', "
            f"date_retour_reelle: {date_ret}, "
            f"statut: '{em['statut']}'}}]->(ex);"
        )

    return "\n".join(lines)


def build_full_cypher(data):
    """Assemble le script Cypher complet"""
    header = """\
// ════════════════════════════════════════════════════════════════════════════
// MIGRATION NEO4J — Système de Gestion de Bibliothèque
// Paradigme : Base de Données Orientée Graphe
// Nœuds     : Livre, Auteur, Membre, Categorie, Exemplaire
// Relations : A_ECRIT, APPARTIENT_A, A_EXEMPLAIRE, A_EMPRUNTE
// ════════════════════════════════════════════════════════════════════════════
"""
    sections = [
        header,
        "// ════ 1. CONTRAINTES ET INDEX ════════════════════════════════════════",
        CYPHER_CONSTRAINTS,
        "// ════ 2. CRÉATION DES NŒUDS ══════════════════════════════════════════",
        build_cypher_nodes(data),
        "// ════ 3. CRÉATION DES RELATIONS ══════════════════════════════════════",
        build_cypher_relations(data),
        "// ════ 4. REQUÊTES ANALYTIQUES ════════════════════════════════════════",
    ]
    for name, cql in CYPHER_QUERIES.items():
        sections.append(f"\n// ── {name} ──")
        sections.append(cql)

    return "\n".join(sections)


# ══════════════════════════════════════════════════════════════════════════════
#  SIMULATION DES RÉSULTATS (sans connexion Neo4j)
# ══════════════════════════════════════════════════════════════════════════════

def simulate_graph_queries(data):
    livres      = data["livres"]
    auteurs     = {a["id"]: a for a in data["auteurs"]}
    categories  = {c["id"]: c for c in data["categories"]}
    membres     = data["membres"]
    exemplaires = data["exemplaires"]
    emprunts    = data["emprunts"]

    results = {}

    # G1 — Livres d'informatique
    info_cat_id = next(c["id"] for c in data["categories"] if c["nom"] == "Informatique")
    results["G1_livres_informatique"] = [
        {
            "titre":  l["titre"],
            "auteur": auteurs[l["auteur_id"]]["nom"] + " " + auteurs[l["auteur_id"]]["prenom"],
            "prix":   l["prix"]
        }
        for l in sorted(livres, key=lambda x: x["titre"])
        if l["categorie_id"] == info_cat_id
    ]

    # G2 — Livres de Fowler
    fowler_id = next(a["id"] for a in data["auteurs"] if a["nom"] == "Fowler")
    results["G2_livres_fowler"] = [
        {
            "titre":    l["titre"],
            "annee":    l["annee_pub"],
            "categorie": categories[l["categorie_id"]]["nom"]
        }
        for l in livres if l["auteur_id"] == fowler_id
    ]

    # G3 — Emprunts en cours
    actifs = []
    for em in emprunts:
        if em["statut"] in ("en_cours", "retard"):
            ex = next(e for e in exemplaires if e["id"] == em["exemplaire_id"])
            l  = next(lv for lv in livres if lv["id"] == ex["livre_id"])
            m  = next(mb for mb in membres if mb["id"] == em["membre_id"])
            actifs.append({
                "membre":  m["prenom"] + " " + m["nom"],
                "livre":   l["titre"],
                "depuis":  em["date_emprunt"],
                "statut":  em["statut"]
            })
    results["G3_emprunts_en_cours"] = sorted(actifs, key=lambda x: x["depuis"])

    # G5 — Retards
    results["G5_retards"] = [
        {
            "membre":  next(mb["prenom"] + " " + mb["nom"] for mb in membres if mb["id"] == em["membre_id"]),
            "livre":   next(lv["titre"] for e in exemplaires if e["id"] == em["exemplaire_id"]
                           for lv in livres if lv["id"] == e["livre_id"]),
            "prevue":  em["date_retour_prevue"],
            "contact": next(mb["email"] for mb in membres if mb["id"] == em["membre_id"])
        }
        for em in emprunts if em["statut"] == "retard"
    ]

    # G6 — Recommandations collaboratives
    # Calculer quels membres ont emprunté quels livres
    membre_livres = {}
    for em in emprunts:
        ex = next(e for e in exemplaires if e["id"] == em["exemplaire_id"])
        livre_id = ex["livre_id"]
        if em["membre_id"] not in membre_livres:
            membre_livres[em["membre_id"]] = set()
        membre_livres[em["membre_id"]].add(livre_id)

    # Co-occurrences de livres
    co_occ = {}
    for member_id, livres_set in membre_livres.items():
        livres_list = list(livres_set)
        for i in range(len(livres_list)):
            for j in range(i + 1, len(livres_list)):
                pair = tuple(sorted([livres_list[i], livres_list[j]]))
                co_occ[pair] = co_occ.get(pair, 0) + 1

    recommandations = []
    for (l1_id, l2_id), count in sorted(co_occ.items(), key=lambda x: -x[1]):
        l1 = next(lv["titre"] for lv in livres if lv["id"] == l1_id)
        l2 = next(lv["titre"] for lv in livres if lv["id"] == l2_id)
        recommandations.append({
            "livre_source":    l1,
            "recommandation":  l2,
            "lecteurs_communs": count
        })
    results["G6_recommandations"] = recommandations[:5]

    # G8 — Centralité des livres
    centralite = []
    for l in livres:
        ex_du_livre = [e for e in exemplaires if e["livre_id"] == l["id"]]
        nb_emp      = sum(1 for em in emprunts if any(e["id"] == em["exemplaire_id"] for e in ex_du_livre))
        centralite.append({
            "titre":            l["titre"],
            "nb_exemplaires":   len(ex_du_livre),
            "nb_emprunts":      nb_emp,
            "taux_utilisation": round(nb_emp / len(ex_du_livre), 2) if ex_du_livre else 0
        })
    results["G8_centralite"] = sorted(centralite, key=lambda x: -x["nb_emprunts"])

    # G9 — Membres connectés par livres communs
    connexions = []
    membres_list = list(membre_livres.keys())
    for i in range(len(membres_list)):
        for j in range(i + 1, len(membres_list)):
            m1_id, m2_id = membres_list[i], membres_list[j]
            communs = membre_livres[m1_id] & membre_livres[m2_id]
            if communs:
                m1 = next(mb for mb in membres if mb["id"] == m1_id)
                m2 = next(mb for mb in membres if mb["id"] == m2_id)
                connexions.append({
                    "membre1":          m1["prenom"] + " " + m1["nom"],
                    "membre2":          m2["prenom"] + " " + m2["nom"],
                    "livres_communs":   [next(lv["titre"] for lv in livres if lv["id"] == lid) for lid in communs],
                    "nb_livres_communs": len(communs)
                })
    results["G9_membres_connectes"] = sorted(connexions, key=lambda x: -x["nb_livres_communs"])

    # G10 — Stats par catégorie (graphe)
    stats_cat = []
    for cat in data["categories"]:
        livres_cat = [l for l in livres if l["categorie_id"] == cat["id"]]
        if not livres_cat:
            continue
        ex_cat = [e for e in exemplaires if any(e["livre_id"] == l["id"] for l in livres_cat)]
        emp_cat = [em for em in emprunts if any(
            e["id"] == em["exemplaire_id"] for e in ex_cat
        )]
        prix_moyen  = sum(l["prix"] for l in livres_cat) / len(livres_cat)
        valeur_stock = sum(l["prix"] * 2 for l in livres_cat)
        stats_cat.append({
            "categorie":      cat["nom"],
            "nb_livres":      len(livres_cat),
            "nb_exemplaires": len(ex_cat),
            "nb_emprunts":    len(emp_cat),
            "prix_moyen":     round(prix_moyen, 2),
            "valeur_stock":   round(valeur_stock, 2)
        })
    results["G10_stats_categories"] = sorted(stats_cat, key=lambda x: -x["nb_livres"])

    # Modèle du graphe — statistiques
    results["graph_model_stats"] = {
        "nb_noeuds": {
            "Livre":      len(livres),
            "Auteur":     len(data["auteurs"]),
            "Membre":     len(membres),
            "Categorie":  len(data["categories"]),
            "Exemplaire": len(exemplaires),
            "TOTAL":      len(livres) + len(data["auteurs"]) + len(membres)
                          + len(data["categories"]) + len(exemplaires)
        },
        "nb_relations": {
            "A_ECRIT":       len(livres),
            "APPARTIENT_A":  len(livres),
            "A_EXEMPLAIRE":  len(exemplaires),
            "A_EMPRUNTE":    len(emprunts),
            "TOTAL":         len(livres) * 2 + len(exemplaires) + len(emprunts)
        }
    }

    return results


# ══════════════════════════════════════════════════════════════════════════════
#  POINT D'ENTRÉE
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    data    = load_data()
    results = simulate_graph_queries(data)
    cypher  = build_full_cypher(data)

    # Écriture du script Cypher
    with open("schema_neo4j.cypher", "w", encoding="utf-8") as f:
        f.write(cypher)

    # Écriture des résultats JSON
    with open("neo4j_queries.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # ── Affichage console ────────────────────────────────────────────────────
    print("═" * 70)
    print("   MIGRATION NEO4J — RÉSULTATS")
    print("═" * 70)

    stats = results["graph_model_stats"]
    print("\n── Modèle du graphe ──")
    print(f"  Nœuds     : {stats['nb_noeuds']}")
    print(f"  Relations : {stats['nb_relations']}")

    print("\n── G1 : Livres d'informatique ──")
    for item in results["G1_livres_informatique"]:
        print(f"  {item['titre'][:40]:40s} | {item['auteur']}")

    print("\n── G3 : Emprunts en cours ──")
    for item in results["G3_emprunts_en_cours"]:
        print(f"  {item['membre']:20s} → {item['livre'][:35]:35s} ({item['statut']})")

    print("\n── G5 : Emprunts en retard ──")
    for item in results["G5_retards"]:
        print(f"  {item['membre']:20s} | {item['livre'][:30]:30s} | prévu: {item['prevue']}")

    print("\n── G6 : Recommandations collaboratives ──")
    for item in results["G6_recommandations"]:
        print(f"  {item['livre_source'][:30]:30s} → {item['recommandation'][:30]:30s} ({item['lecteurs_communs']} lecteurs)")

    print("\n── G8 : Centralité des livres ──")
    for item in results["G8_centralite"]:
        print(f"  {item['titre'][:40]:40s} | emprunts: {item['nb_emprunts']} | taux: {item['taux_utilisation']}")

    print("\n── G9 : Réseau de lecture (membres connectés) ──")
    for item in results["G9_membres_connectes"]:
        print(f"  {item['membre1']:20s} ↔ {item['membre2']:20s} | livres communs: {item['livres_communs']}")

    print("\n── G10 : Statistiques par catégorie (graphe) ──")
    for item in results["G10_stats_categories"]:
        print(f"  {item['categorie']:15s} | livres: {item['nb_livres']} | emprunts: {item['nb_emprunts']} | stock: {item['valeur_stock']} TND")

    print(f"\n[OK] Script Cypher complet  → schema_neo4j.cypher")
    print(f"[OK] Résultats JSON         → neo4j_queries.json")
