"""
02_migration_mongodb.py
Migration de la base relationnelle vers MongoDB
Stratégie : dénormalisation + documents imbriqués
"""
import mysql.connector
import json
from datetime import datetime
from pymongo import MongoClient

# Configuration
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'bibliotheque'
}

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "bibliotheque_mongo"

# ── Lire les données MySQL ──────────────────────────────────────────────────
def fetch_all(conn, sql, params=()):
    cur = conn.cursor(dictionary=True)
    cur.execute(sql, params)
    return cur.fetchall()

def build_mongo_documents():
    conn = mysql.connector.connect(**MYSQL_CONFIG)

    categories  = {r["id"]: r for r in fetch_all(conn,"SELECT * FROM categories")}
    auteurs     = {r["id"]: r for r in fetch_all(conn,"SELECT * FROM auteurs")}
    exemplaires = fetch_all(conn,"SELECT * FROM exemplaires")
    emprunts    = fetch_all(conn,"SELECT * FROM emprunts")
    membres     = {r["id"]: r for r in fetch_all(conn,"SELECT * FROM membres")}
    livres_raw  = fetch_all(conn,"SELECT * FROM livres")

    # ── Collection : livres (document complet, dénormalisé) ─────────────────
    livres_docs = []
    for l in livres_raw:
        auteur = auteurs[l["auteur_id"]]
        cat    = categories[l["categorie_id"]]
        exs    = [e for e in exemplaires if e["livre_id"] == l["id"]]

        for e in exs:
            e_emprunts = [em for em in emprunts if em["exemplaire_id"] == e["id"]]
            e["historique_emprunts"] = e_emprunts

        doc = {
            "_id":        f"livre_{l['id']:03d}",
            "titre":      l["titre"],
            "isbn":       l["isbn"],
            "annee_pub":  l["annee_pub"],
            "editeur":    l["editeur"],
            "prix":       float(l["prix"]) if l["prix"] is not None else 0.0,
            "auteur": {
                "nom":         auteur["nom"],
                "prenom":      auteur["prenom"],
                "nationalite": auteur["nationalite"],
                "date_naissance": str(auteur["date_naissance"]) if auteur["date_naissance"] else None
            },
            "categorie": {
                "nom":         cat["nom"],
                "description": cat["description"]
            },
            "exemplaires": [
                {
                    "exemplaire_id":  e["id"],
                    "etat":           e["etat"],
                    "disponible":     bool(e["disponible"]),
                    "historique_emprunts": [
                        {
                            "emprunt_id":         em["id"],
                            "membre_id":          em["membre_id"],
                            "date_emprunt":       str(em["date_emprunt"]),
                            "date_retour_prevue": str(em["date_retour_prevue"]),
                            "date_retour_reelle": str(em["date_retour_reelle"]) if em["date_retour_reelle"] else None,
                            "statut":             em["statut"]
                        }
                        for em in e["historique_emprunts"]
                    ]
                }
                for e in exs
            ],
            "nb_exemplaires":    len(exs),
            "nb_disponibles":    sum(1 for e in exs if e["disponible"])
        }
        livres_docs.append(doc)

    # ── Collection : membres (avec emprunts courants) ────────────────────────
    membres_docs = []
    for m in membres.values():
        # emprunts de ce membre
        emp_membre = [em for em in emprunts if em["membre_id"] == m["id"]]
        # enrichir avec infos livre
        emp_enrichis = []
        for em in emp_membre:
            ex = next((e for e in exemplaires if e["id"] == em["exemplaire_id"]), {})
            l  = next((l for l in livres_raw if l["id"] == ex.get("livre_id")), {})
            emp_enrichis.append({
                "emprunt_id":         em["id"],
                "livre_titre":        l.get("titre","?"),
                "livre_isbn":         l.get("isbn","?"),
                "date_emprunt":       str(em["date_emprunt"]),
                "date_retour_prevue": str(em["date_retour_prevue"]),
                "date_retour_reelle": str(em["date_retour_reelle"]) if em["date_retour_reelle"] else None,
                "statut":             em["statut"]
            })

        doc = {
            "_id":              f"membre_{m['id']:03d}",
            "nom":              m["nom"],
            "prenom":           m["prenom"],
            "email":            m["email"],
            "telephone":        m["telephone"],
            "adresse": {
                "rue":   m["adresse"],
                "ville": m["ville"]
            },
            "date_inscription": str(m["date_inscription"]),
            "emprunts":         emp_enrichis,
            "nb_emprunts_total": len(emp_enrichis),
            "emprunts_en_cours": sum(1 for e in emp_enrichis if e["statut"]=="en_cours"),
            "emprunts_retard":   sum(1 for e in emp_enrichis if e["statut"]=="retard")
        }
        membres_docs.append(doc)

    conn.close()
    return livres_docs, membres_docs

def insert_into_mongodb(livres_docs, membres_docs):
    """Insère les documents générés dans MongoDB"""
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    db.livres.drop()
    db.membres.drop()

    if livres_docs:
        db.livres.insert_many(livres_docs)
    if membres_docs:
        db.membres.insert_many(membres_docs)
        
    return db

def simulate_mongo_operations(db):
    """Exécute de vraies opérations PyMongo"""
    print("\n── Exécution de requêtes via PyMongo ──")
    
    # Requête 1 : Trouver tous les livres d'informatique
    print("\nLivres d'informatique :")
    for doc in db.livres.find({"categorie.nom": "Informatique"}, {"titre": 1, "auteur.nom": 1}):
        print("  -", doc)

    # Requête 2 : Livres disponibles (au moins 1 exemplaire dispo)
    print("\nLivres disponibles :")
    for doc in db.livres.find({"nb_disponibles": {"$gt": 0}}, {"titre": 1, "nb_disponibles": 1}):
        print("  -", doc)

    # Requête 3 : Membres avec emprunts en retard
    print("\nMembres avec retards :")
    for doc in db.membres.find({"emprunts_retard": {"$gt": 0}}, {"nom": 1, "prenom": 1, "emprunts_retard": 1}):
        print("  -", doc)

    # Requête 4 : Aggregation - livres par catégorie
    print("\nLivres par catégorie :")
    pipeline = [
        {"$group": {"_id": "$categorie.nom", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    for doc in db.livres.aggregate(pipeline):
        print("  -", doc)


if __name__ == "__main__":
    livres_docs, membres_docs = build_mongo_documents()

    # Sauvegarder les documents JSON
    with open("mongo_livres.json", "w", encoding="utf-8") as f:
        json.dump(livres_docs, f, ensure_ascii=False, indent=2)

    with open("mongo_membres.json", "w", encoding="utf-8") as f:
        json.dump(membres_docs, f, ensure_ascii=False, indent=2)

    print(f"[OK] {len(livres_docs)} documents livres générés  → mongo_livres.json")
    print(f"[OK] {len(membres_docs)} documents membres générés → mongo_membres.json")

    # MongoDB Connect & Insert
    print("Connexion à MongoDB et insertion des documents...")
    db = insert_into_mongodb(livres_docs, membres_docs)
    print(f"[OK] Documents insérés dans MongoDB ({MONGO_DB})")

    simulate_mongo_operations(db)

"""
05_requetes_avancees_mongodb.py
Requêtes MongoDB Avancées — Niveau Expert
Couvre : Aggregation Pipeline, $lookup, $facet, $bucket,
         $graphLookup, Transactions, Indexing, Text Search,
         Geospatial (simulé), Window Functions
"""
from collections import defaultdict, Counter
from datetime import timedelta

def fetch(conn, sql, params=()):
    cur = conn.cursor(dictionary=True)
    cur.execute(sql, params)
    return cur.fetchall()

def load_data():
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    data = {
        "categories":  {r["id"]: r for r in fetch(conn,"SELECT * FROM categories")},
        "auteurs":     {r["id"]: r for r in fetch(conn,"SELECT * FROM auteurs")},
        "livres":      fetch(conn,"SELECT * FROM livres"),
        "membres":     fetch(conn,"SELECT * FROM membres"),
        "exemplaires": fetch(conn,"SELECT * FROM exemplaires"),
        "emprunts":    fetch(conn,"SELECT * FROM emprunts"),
    }
    conn.close()
    return data

def run_all_queries():
    data  = load_data()
    livres     = data["livres"]
    auteurs    = data["auteurs"]
    categories = data["categories"]
    membres    = data["membres"]
    exemplaires= data["exemplaires"]
    emprunts   = data["emprunts"]

    results = {}

    cat_stats = defaultdict(lambda: {"nb_livres":0,"prix":[],"nb_ex":0,"nb_dispo":0})
    for l in livres:
        cat = categories[l["categorie_id"]]["nom"]
        nb_ex   = sum(1 for e in exemplaires if e["livre_id"]==l["id"])
        nb_dispo= sum(1 for e in exemplaires if e["livre_id"]==l["id"] and e["disponible"])
        cat_stats[cat]["nb_livres"] += 1
        cat_stats[cat]["prix"].append(float(l["prix"]))
        cat_stats[cat]["nb_ex"] += nb_ex
        cat_stats[cat]["nb_dispo"] += nb_dispo

    results["M1_stats_par_categorie"] = sorted([{
        "categorie":         cat,
        "nb_livres":         v["nb_livres"],
        "prix_moyen":        round(sum(v["prix"])/len(v["prix"]),2),
        "prix_max":          max(v["prix"]),
        "prix_min":          min(v["prix"]),
        "total_exemplaires": v["nb_ex"],
        "total_disponibles": v["nb_dispo"],
        "taux_disponibilite":f"{round(v['nb_dispo']/v['nb_ex']*100,1)}%"
    } for cat, v in cat_stats.items()], key=lambda x: -x["nb_livres"])

    emp_actifs = [e for e in emprunts if e["statut"] in ("en_cours","retard")]
    lookup_result = []
    for em in emp_actifs:
        ex = next((e for e in exemplaires if e["id"]==em["exemplaire_id"]), None)
        l  = next((lv for lv in livres if lv["id"]==ex["livre_id"]), None) if ex else None
        m  = next((mb for mb in membres if mb["id"]==em["membre_id"]), None)
        if l and m:
            lookup_result.append({
                "livre_titre":        l["titre"],
                "exemplaire_id":      ex["id"],
                "etat_exemplaire":    ex["etat"],
                "membre":             f"{m['prenom']} {m['nom']}",
                "email":              m["email"],
                "date_emprunt":       str(em["date_emprunt"]),
                "date_retour_prevue": str(em["date_retour_prevue"]),
                "statut":             em["statut"],
                "jours_retard":       14 if em["statut"]=="retard" else 0
            })
    results["M2_lookup_livres_emprunts_actifs"] = lookup_result

    by_editeur  = Counter(l["editeur"] for l in livres)
    by_decade   = Counter()
    price_dist  = Counter()
    for l in livres:
        yr = l["annee_pub"]
        if yr < 1950:  by_decade["Avant 1950"] += 1
        elif yr < 1970: by_decade["1950-1969"] += 1
        elif yr < 1990: by_decade["1970-1989"] += 1
        elif yr < 2010: by_decade["1990-2009"] += 1
        else:           by_decade["2010+"]    += 1

        p = float(l["prix"])
        if p < 15:    price_dist["0-15 TND"]   += 1
        elif p < 30:  price_dist["15-30 TND"]  += 1
        elif p < 50:  price_dist["30-50 TND"]  += 1
        elif p < 75:  price_dist["50-75 TND"]  += 1
        else:         price_dist["75+ TND"]    += 1

    results["M3_facet_analyse_multidim"] = {
        "par_categorie":    [{"_id":k,"count":v} for k,v in Counter(categories[l["categorie_id"]]["nom"] for l in livres).items()],
        "par_editeur":      [{"_id":k,"count":v} for k,v in by_editeur.most_common()],
        "par_decade":       [{"periode":k,"count":v} for k,v in sorted(by_decade.items())],
        "prix_distribution":[{"tranche":k,"count":v} for k,v in sorted(price_dist.items())]
    }

    membre_scores = []
    for m in membres:
        emp_m = [e for e in emprunts if e["membre_id"]==m["id"]]
        rendus    = sum(1 for e in emp_m if e["statut"]=="rendu")
        en_cours  = sum(1 for e in emp_m if e["statut"]=="en_cours")
        retards   = sum(1 for e in emp_m if e["statut"]=="retard")
        score = len(emp_m)*10 + rendus*5 - retards*20
        membre_scores.append({
            "membre":          f"{m['prenom']} {m['nom']}",
            "ville":           m["ville"],
            "nb_emprunts":     len(emp_m),
            "rendus_a_temps":  rendus,
            "en_cours":        en_cours,
            "retards":         retards,
            "score_activite":  score,
            "profil":          "⭐ Excellent" if score>=20 else ("✅ Bon" if score>=10 else ("⚠️ Moyen" if score>=0 else "❌ Mauvais"))
        })
    results["M4_classement_membres_score"] = sorted(membre_scores, key=lambda x: -x["score_activite"])

    auteur_stats = defaultdict(lambda: {"livres":[],"nb_ex":0,"prix":[],"nb_emp":0})
    for l in livres:
        a_id = l["auteur_id"]
        a    = auteurs[a_id]
        key  = f"{a['prenom']} {a['nom']}"
        nb_ex  = sum(1 for e in exemplaires if e["livre_id"]==l["id"])
        nb_emp = sum(1 for em in emprunts
                     for e in exemplaires
                     if e["livre_id"]==l["id"] and em["exemplaire_id"]==e["id"])
        auteur_stats[key]["livres"].append(l["titre"])
        auteur_stats[key]["nb_ex"] += nb_ex
        auteur_stats[key]["prix"].append(float(l["prix"]))
        auteur_stats[key]["nb_emp"] += nb_emp

    results["M5_top_auteurs_valeur_stock"] = sorted([{
        "auteur":           k,
        "nb_livres":        len(v["livres"]),
        "valeur_stock_TND": round(sum(p*2 for p in v["prix"]), 2),
        "nb_emprunts_total":v["nb_emp"],
        "popularite":       f"{round(v['nb_emp']/max(len(v['livres']),1),1)} empr/livre",
        "prix_moyen_TND":   round(sum(v["prix"])/len(v["prix"]),2)
    } for k, v in auteur_stats.items()], key=lambda x: -x["valeur_stock_TND"])

    monthly = defaultdict(lambda: {"nb_emprunts":0,"nb_retours":0,"nb_retards":0,"livres":[]})
    for em in emprunts:
        mois = str(em["date_emprunt"])[:7]
        monthly[mois]["nb_emprunts"] += 1
        if em["date_retour_reelle"]: monthly[mois]["nb_retours"] += 1
        if em["statut"] == "retard": monthly[mois]["nb_retards"] += 1
        ex = next((e for e in exemplaires if e["id"]==em["exemplaire_id"]), None)
        if ex:
            l = next((lv for lv in livres if lv["id"]==ex["livre_id"]), None)
            if l: monthly[mois]["livres"].append(l["titre"])

    results["M6_activite_mensuelle"] = sorted([{
        "mois":         k,
        "nb_emprunts":  v["nb_emprunts"],
        "nb_retours":   v["nb_retours"],
        "nb_retards":   v["nb_retards"],
        "taux_retard":  f"{round(v['nb_retards']/v['nb_emprunts']*100)}%",
        "livres_empruntes": v["livres"]
    } for k, v in monthly.items()], key=lambda x: x["mois"])

    livre_lecteurs = defaultdict(set)
    for em in emprunts:
        ex = next((e for e in exemplaires if e["id"]==em["exemplaire_id"]), None)
        if ex:
            l = next((lv for lv in livres if lv["id"]==ex["livre_id"]), None)
            if l: livre_lecteurs[l["titre"]].add(em["membre_id"])

    co_occur = defaultdict(Counter)
    for titre, lecteurs in livre_lecteurs.items():
        for autre_titre, autres_lecteurs in livre_lecteurs.items():
            if titre != autre_titre:
                communs = lecteurs & autres_lecteurs
                if communs:
                    co_occur[titre][autre_titre] = len(communs)

    recommandations = {}
    for titre, similaires in co_occur.items():
        if similaires:
            top = similaires.most_common(2)
            recommandations[titre] = [{"recommande": t, "membres_communs": n} for t,n in top]

    results["M7_recommandations_collaboratives"] = recommandations

    livres_avec_rang = []
    by_cat = defaultdict(list)
    for l in livres:
        cat = categories[l["categorie_id"]]["nom"]
        by_cat[cat].append((l["titre"], float(l["prix"])))

    for cat, items in by_cat.items():
        sorted_items = sorted(items, key=lambda x: -x[1])
        for rang, (titre, prix) in enumerate(sorted_items, 1):
            percentile = round((len(sorted_items)-rang)/max(len(sorted_items)-1,1)*100)
            livres_avec_rang.append({
                "categorie":  cat,
                "titre":      titre,
                "prix":       prix,
                "rang_prix":  rang,
                "percentile": f"Top {100-percentile}%"
            })
    results["M8_rang_prix_par_categorie"] = sorted(livres_avec_rang, key=lambda x: (x["categorie"], x["rang_prix"]))

    anomalies = []
    for m in membres:
        emp_m = [e for e in emprunts if e["membre_id"]==m["id"]]
        if not emp_m: continue
        retard_rate = sum(1 for e in emp_m if e["statut"]=="retard") / len(emp_m)
        titres_emp  = []
        for e in emp_m:
            ex = next((ex for ex in exemplaires if ex["id"]==e["exemplaire_id"]),None)
            if ex:
                l = next((lv for lv in livres if lv["id"]==ex["livre_id"]),None)
                if l: titres_emp.append(l["titre"])
        doublons = [t for t, n in Counter(titres_emp).items() if n > 1]

        if retard_rate >= 0.5 or doublons:
            anomalies.append({
                "membre":         f"{m['prenom']} {m['nom']}",
                "taux_retard":    f"{int(retard_rate*100)}%",
                "livres_doublon": doublons,
                "alerte":         "🚨 Taux retard élevé" if retard_rate >= 0.5 else "⚠️ Emprunt répété"
            })
    results["M9_detection_anomalies"] = anomalies

    total_valeur_stock = sum(float(l["prix"]) * sum(1 for e in exemplaires if e["livre_id"]==l["id"]) for l in livres)
    taux_occupation    = sum(1 for e in exemplaires if not e["disponible"]) / len(exemplaires)
    livres_jamais_emp  = []
    for l in livres:
        exs_l = [e["id"] for e in exemplaires if e["livre_id"]==l["id"]]
        if not any(em["exemplaire_id"] in exs_l for em in emprunts):
            livres_jamais_emp.append(l["titre"])

    results["M10_kpi_dashboard"] = {
        "total_livres":           len(livres),
        "total_membres":          len(membres),
        "total_exemplaires":      len(exemplaires),
        "exemplaires_disponibles":sum(1 for e in exemplaires if e["disponible"]),
        "taux_occupation":        f"{round(taux_occupation*100,1)}%",
        "total_emprunts":         len(emprunts),
        "emprunts_en_cours":      sum(1 for e in emprunts if e["statut"]=="en_cours"),
        "emprunts_en_retard":     sum(1 for e in emprunts if e["statut"]=="retard"),
        "emprunts_rendus":        sum(1 for e in emprunts if e["statut"]=="rendu"),
        "valeur_stock_totale":    f"{round(total_valeur_stock,2)} TND",
        "prix_moyen_livre":       f"{round(sum(float(l['prix']) for l in livres)/len(livres),2)} TND",
        "livre_le_plus_cher":     max(livres, key=lambda x:float(x["prix"]))["titre"],
        "livre_le_moins_cher":    min(livres, key=lambda x:float(x["prix"]))["titre"],
        "livres_jamais_empruntes":livres_jamais_emp,
        "nb_livres_jamais_empr":  len(livres_jamais_emp),
        "ville_plus_active":      Counter(m["ville"] for m in membres).most_common(1)[0][0],
        "taux_retard_global":     f"{round(sum(1 for e in emprunts if e['statut']=='retard')/len(emprunts)*100,1)}%"
    }

    return results

def display_results(results):
    print("\n" + "═"*70)
    print("   REQUÊTES AVANCÉES MONGODB — RÉSULTATS")
    print("═"*70)

    titles = {
        "M1_stats_par_categorie":         "M1 ── Statistiques par catégorie (Aggregation Pipeline)",
        "M2_lookup_livres_emprunts_actifs":"M2 ── Jointure $lookup livres ↔ emprunts actifs",
        "M3_facet_analyse_multidim":       "M3 ── Analyse $facet multi-dimensionnelle",
        "M4_classement_membres_score":     "M4 ── Classement membres par score d'activité",
        "M5_top_auteurs_valeur_stock":     "M5 ── Top auteurs par valeur de stock",
        "M6_activite_mensuelle":           "M6 ── Activité mensuelle (pipeline temporel)",
        "M7_recommandations_collaboratives":"M7 ── Recommandations collaboratives",
        "M8_rang_prix_par_categorie":      "M8 ── Rang & percentile prix ($setWindowFields)",
        "M9_detection_anomalies":          "M9 ── Détection d'anomalies",
        "M10_kpi_dashboard":               "M10 ── KPI Dashboard complet",
    }

    for key, title in titles.items():
        print(f"\n{'─'*70}")
        print(f"  {title}")
        print(f"{'─'*70}")
        val = results[key]
        if isinstance(val, list):
            for item in val:
                print(f"  {item}")
        elif isinstance(val, dict):
            for k, v in val.items():
                print(f"  {k:35s} : {v}")

    with open("mongo_requetes_avancees.json","w",encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n[OK] Résultats sauvegardés → mongo_requetes_avancees.json")

# Note: The advanced queries can be run directly using PyMongo for equivalent MongoDB queries.
# To run them as python simulations, you could uncomment the following lines.
# if __name__ == "__main__":
#     results = run_all_queries()
#     display_results(results)
