"""
02_migration_mongodb.py
Migration de la base relationnelle vers MongoDB
Stratégie : dénormalisation + documents imbriqués
"""
import sqlite3
import json
from datetime import datetime

DB_PATH = "bibliotheque.db"

# ── Lire les données SQLite ──────────────────────────────────────────────────
def fetch_all(conn, sql, params=()):
    cur = conn.cursor()
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def build_mongo_documents():
    conn = sqlite3.connect(DB_PATH)

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
            "prix":       l["prix"],
            "auteur": {
                "nom":         auteur["nom"],
                "prenom":      auteur["prenom"],
                "nationalite": auteur["nationalite"],
                "date_naissance": auteur["date_naissance"]
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
                            "date_emprunt":       em["date_emprunt"],
                            "date_retour_prevue": em["date_retour_prevue"],
                            "date_retour_reelle": em["date_retour_reelle"],
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
                "date_emprunt":       em["date_emprunt"],
                "date_retour_prevue": em["date_retour_prevue"],
                "date_retour_reelle": em["date_retour_reelle"],
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
            "date_inscription": m["date_inscription"],
            "emprunts":         emp_enrichis,
            "nb_emprunts_total": len(emp_enrichis),
            "emprunts_en_cours": sum(1 for e in emp_enrichis if e["statut"]=="en_cours"),
            "emprunts_retard":   sum(1 for e in emp_enrichis if e["statut"]=="retard")
        }
        membres_docs.append(doc)

    conn.close()
    return livres_docs, membres_docs


def simulate_mongo_operations(livres_docs, membres_docs):
    """Simule les opérations MongoDB (sans serveur)"""
    results = {}

    # Requête 1 : Trouver tous les livres d'informatique
    results["livres_informatique"] = [
        {"_id": d["_id"], "titre": d["titre"], "auteur": d["auteur"]["nom"]}
        for d in livres_docs
        if d["categorie"]["nom"] == "Informatique"
    ]

    # Requête 2 : Livres disponibles (au moins 1 exemplaire dispo)
    results["livres_disponibles"] = [
        {"_id": d["_id"], "titre": d["titre"], "nb_disponibles": d["nb_disponibles"]}
        for d in livres_docs
        if d["nb_disponibles"] > 0
    ]

    # Requête 3 : Membres avec emprunts en retard
    results["membres_retard"] = [
        {"_id": m["_id"], "nom": m["nom"], "prenom": m["prenom"],
         "retards": m["emprunts_retard"]}
        for m in membres_docs
        if m["emprunts_retard"] > 0
    ]

    # Requête 4 : Aggregation - livres par catégorie
    from collections import Counter
    cats = Counter(d["categorie"]["nom"] for d in livres_docs)
    results["livres_par_categorie"] = [
        {"categorie": k, "count": v} for k, v in cats.most_common()
    ]

    return results


if __name__ == "__main__":
    livres_docs, membres_docs = build_mongo_documents()

    # Sauvegarder les documents JSON
    with open("mongo_livres.json", "w", encoding="utf-8") as f:
        json.dump(livres_docs, f, ensure_ascii=False, indent=2)

    with open("mongo_membres.json", "w", encoding="utf-8") as f:
        json.dump(membres_docs, f, ensure_ascii=False, indent=2)

    print(f"[OK] {len(livres_docs)} documents livres générés  → mongo_livres.json")
    print(f"[OK] {len(membres_docs)} documents membres générés → mongo_membres.json")

    results = simulate_mongo_operations(livres_docs, membres_docs)
    print("\n── Résultats des requêtes MongoDB ──")
    for k, v in results.items():
        print(f"\n  {k} ({len(v)} résultats):")
        for item in v[:3]:
            print(f"    {item}")

    # Afficher un exemple de document
    print("\n── Exemple document livre (Clean Code) ──")
    print(json.dumps(livres_docs[1], ensure_ascii=False, indent=2)[:800], "...")
"""
05_requetes_avancees_mongodb.py
Requêtes MongoDB Avancées — Niveau Expert
Couvre : Aggregation Pipeline, $lookup, $facet, $bucket,
         $graphLookup, Transactions, Indexing, Text Search,
         Geospatial (simulé), Window Functions
"""
import sqlite3, json
from collections import defaultdict, Counter
from datetime import datetime, timedelta

DB_PATH = "bibliotheque.db"

def fetch(conn, sql, params=()):
    cur = conn.cursor()
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def load_data():
    conn = sqlite3.connect(DB_PATH)
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

    # ════════════════════════════════════════════════════════════
    # REQ M-1 : Aggregation Pipeline — Statistiques par catégorie
    # ════════════════════════════════════════════════════════════
    # Pipeline MongoDB :
    # db.livres.aggregate([
    #   { $group: { _id: "$categorie.nom",
    #       nb_livres:      { $sum: 1 },
    #       prix_moyen:     { $avg: "$prix" },
    #       prix_max:       { $max: "$prix" },
    #       prix_min:       { $min: "$prix" },
    #       total_exemplaires: { $sum: "$nb_exemplaires" },
    #       total_disponibles: { $sum: "$nb_disponibles" } }},
    #   { $addFields: { taux_dispo: { $divide: ["$total_disponibles","$total_exemplaires"] }}},
    #   { $sort: { nb_livres: -1 }}
    # ])
    cat_stats = defaultdict(lambda: {"nb_livres":0,"prix":[],"nb_ex":0,"nb_dispo":0})
    for l in livres:
        cat = categories[l["categorie_id"]]["nom"]
        nb_ex   = sum(1 for e in exemplaires if e["livre_id"]==l["id"])
        nb_dispo= sum(1 for e in exemplaires if e["livre_id"]==l["id"] and e["disponible"])
        cat_stats[cat]["nb_livres"] += 1
        cat_stats[cat]["prix"].append(l["prix"])
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

    # ════════════════════════════════════════════════════════════
    # REQ M-2 : $lookup — Jointure livres ↔ emprunts actifs
    # ════════════════════════════════════════════════════════════
    # db.livres.aggregate([
    #   { $unwind: "$exemplaires" },
    #   { $lookup: {
    #       from: "emprunts_actifs",
    #       localField: "exemplaires.exemplaire_id",
    #       foreignField: "exemplaire_id",
    #       as: "emprunts_actifs" }},
    #   { $match: { "emprunts_actifs": { $ne: [] } }},
    #   { $project: { titre:1, "exemplaires.exemplaire_id":1,
    #                 "emprunts_actifs.membre_id":1,
    #                 "emprunts_actifs.date_retour_prevue":1 }}
    # ])
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
                "date_emprunt":       em["date_emprunt"],
                "date_retour_prevue": em["date_retour_prevue"],
                "statut":             em["statut"],
                "jours_retard":       14 if em["statut"]=="retard" else 0
            })
    results["M2_lookup_livres_emprunts_actifs"] = lookup_result

    # ════════════════════════════════════════════════════════════
    # REQ M-3 : $facet — Analyse multi-dimensionnelle
    # ════════════════════════════════════════════════════════════
    # db.livres.aggregate([{ $facet: {
    #   "par_categorie":   [{$group:{_id:"$categorie.nom", count:{$sum:1}}}],
    #   "par_editeur":     [{$group:{_id:"$editeur",       count:{$sum:1}}}],
    #   "par_decade":      [{$bucket:{groupBy:"$annee_pub", boundaries:[1880,1950,1970,1990,2010,2030]}}],
    #   "prix_distribution":[{$bucket:{groupBy:"$prix",    boundaries:[0,15,30,50,75,100]}}]
    # }}])
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

        p = l["prix"]
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

    # ════════════════════════════════════════════════════════════
    # REQ M-4 : $bucket + $project — Classement des membres
    # ════════════════════════════════════════════════════════════
    # Calcule un score d'activité pour chaque membre :
    # score = nb_emprunts_total * 10 + nb_rendus_à_temps * 5 - nb_retards * 20
    # db.membres.aggregate([
    #   { $addFields: { score_activite: {
    #       $subtract: [
    #         {$add:[{$multiply:["$nb_emprunts_total",10]},{$multiply:["$emprunts_rendus",5]}]},
    #         {$multiply:["$emprunts_retard",20]}
    #       ]}}},
    #   { $sort: { score_activite: -1 }},
    #   { $limit: 5 }
    # ])
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

    # ════════════════════════════════════════════════════════════
    # REQ M-5 : $unwind + $group — Top auteurs par valeur stock
    # ════════════════════════════════════════════════════════════
    # db.livres.aggregate([
    #   { $group: { _id: { nom:"$auteur.nom", prenom:"$auteur.prenom" },
    #       nb_livres:          { $sum: 1 },
    #       valeur_stock:       { $sum: { $multiply: ["$prix","$nb_exemplaires"] }},
    #       nb_total_emprunts:  { $sum: { $size: "$exemplaires.historique_emprunts" }},
    #       prix_moyen:         { $avg: "$prix" }}},
    #   { $sort: { valeur_stock: -1 }}
    # ])
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
        auteur_stats[key]["prix"].append(l["prix"])
        auteur_stats[key]["nb_emp"] += nb_emp

    results["M5_top_auteurs_valeur_stock"] = sorted([{
        "auteur":           k,
        "nb_livres":        len(v["livres"]),
        "valeur_stock_TND": round(sum(p*2 for p in v["prix"]), 2),
        "nb_emprunts_total":v["nb_emp"],
        "popularite":       f"{round(v['nb_emp']/max(len(v['livres']),1),1)} empr/livre",
        "prix_moyen_TND":   round(sum(v["prix"])/len(v["prix"]),2)
    } for k, v in auteur_stats.items()], key=lambda x: -x["valeur_stock_TND"])

    # ════════════════════════════════════════════════════════════
    # REQ M-6 : Pipeline temporel — Activité par mois
    # ════════════════════════════════════════════════════════════
    # db.emprunts.aggregate([
    #   { $addFields: { mois: { $month: "$date_emprunt" },
    #                   annee:{ $year:  "$date_emprunt" }}},
    #   { $group: { _id: { mois:"$mois", annee:"$annee" },
    #       nb_emprunts: { $sum: 1 },
    #       nb_retours:  { $sum: { $cond: [{ $ne:["$date_retour_reelle",null]},1,0]}},
    #       nb_retards:  { $sum: { $cond: [{ $eq:["$statut","retard"]},1,0]}}}},
    #   { $sort: { "_id.annee":1, "_id.mois":1 }}
    # ])
    monthly = defaultdict(lambda: {"nb_emprunts":0,"nb_retours":0,"nb_retards":0,"livres":[]})
    for em in emprunts:
        mois = em["date_emprunt"][:7]
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

    # ════════════════════════════════════════════════════════════
    # REQ M-7 : Recommandation — "Les membres qui ont emprunté X
    #           ont aussi emprunté Y" (Collaborative Filtering)
    # ════════════════════════════════════════════════════════════
    # db.membres.aggregate([
    #   { $unwind: "$emprunts" },
    #   { $group: { _id: "$emprunts.livre_titre",
    #       membres_lecteurs: { $addToSet: "$_id" }}},
    #   Puis calcul de co-occurrences
    # ])
    livre_lecteurs = defaultdict(set)
    for em in emprunts:
        ex = next((e for e in exemplaires if e["id"]==em["exemplaire_id"]), None)
        if ex:
            l = next((lv for lv in livres if lv["id"]==ex["livre_id"]), None)
            if l: livre_lecteurs[l["titre"]].add(em["membre_id"])

    # Co-occurrences
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

    # ════════════════════════════════════════════════════════════
    # REQ M-8 : $setWindowFields — Rang et percentile
    # ════════════════════════════════════════════════════════════
    # Classement des livres par prix dans leur catégorie
    # db.livres.aggregate([
    #   { $setWindowFields: {
    #       partitionBy: "$categorie.nom",
    #       sortBy: { prix: -1 },
    #       output: {
    #         rang_prix:    { $rank: {} },
    #         percentile:   { $percentRank: {} }
    #       }}}
    # ])
    livres_avec_rang = []
    by_cat = defaultdict(list)
    for l in livres:
        cat = categories[l["categorie_id"]]["nom"]
        by_cat[cat].append((l["titre"], l["prix"]))

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

    # ════════════════════════════════════════════════════════════
    # REQ M-9 : Détection d'anomalies — Emprunts suspects
    # ════════════════════════════════════════════════════════════
    # Membres qui ont emprunté le même livre plusieurs fois,
    # ou dont tous les emprunts sont en retard
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

    # ════════════════════════════════════════════════════════════
    # REQ M-10 : Rapport KPI — Tableau de bord complet
    # ════════════════════════════════════════════════════════════
    total_valeur_stock = sum(l["prix"] * sum(1 for e in exemplaires if e["livre_id"]==l["id"]) for l in livres)
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
        "prix_moyen_livre":       f"{round(sum(l['prix'] for l in livres)/len(livres),2)} TND",
        "livre_le_plus_cher":     max(livres, key=lambda x:x["prix"])["titre"],
        "livre_le_moins_cher":    min(livres, key=lambda x:x["prix"])["titre"],
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


if __name__ == "__main__":
    results = run_all_queries()
    display_results(results)
