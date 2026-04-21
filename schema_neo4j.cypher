// ════════════════════════════════════════════════════════════════════════════
// MIGRATION NEO4J — Système de Gestion de Bibliothèque
// Paradigme : Base de Données Orientée Graphe
// Nœuds     : Livre, Auteur, Membre, Categorie, Exemplaire
// Relations : A_ECRIT, APPARTIENT_A, A_EXEMPLAIRE, A_EMPRUNTE
// ════════════════════════════════════════════════════════════════════════════

// ════ 1. CONTRAINTES ET INDEX ════════════════════════════════════════

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

// ════ 2. CRÉATION DES NŒUDS ══════════════════════════════════════════

// ─── Nœuds Categorie ─────────────────────────────────────
MERGE (:Categorie {nom: 'Informatique', description: 'Livres sur la programmation et les systèmes'});
MERGE (:Categorie {nom: 'Sciences', description: 'Mathématiques, physique, biologie'});
MERGE (:Categorie {nom: 'Littérature', description: 'Romans, nouvelles, poésie'});
MERGE (:Categorie {nom: 'Histoire', description: 'Histoire mondiale et locale'});
MERGE (:Categorie {nom: 'Philosophie', description: 'Pensée critique et philosophie'});

// ─── Nœuds Auteur ────────────────────────────────────────
MERGE (:Auteur {auteur_id: 1, nom: 'Knuth', prenom: 'Donald', nationalite: 'Américain', date_naissance: '1938-01-10'});
MERGE (:Auteur {auteur_id: 2, nom: 'Martin', prenom: 'Robert', nationalite: 'Américain', date_naissance: '1952-12-05'});
MERGE (:Auteur {auteur_id: 3, nom: 'Codd', prenom: 'Edgar', nationalite: 'Britannique', date_naissance: '1923-08-19'});
MERGE (:Auteur {auteur_id: 4, nom: 'Camus', prenom: 'Albert', nationalite: 'Français', date_naissance: '1913-11-07'});
MERGE (:Auteur {auteur_id: 5, nom: 'Tanenbaum', prenom: 'Andrew', nationalite: 'Américain', date_naissance: '1944-03-16'});
MERGE (:Auteur {auteur_id: 6, nom: 'Fowler', prenom: 'Martin', nationalite: 'Britannique', date_naissance: '1963-06-18'});
MERGE (:Auteur {auteur_id: 7, nom: 'García M.', prenom: 'Gabriel', nationalite: 'Colombien', date_naissance: '1927-03-06'});
MERGE (:Auteur {auteur_id: 8, nom: 'Nietzsche', prenom: 'Friedrich', nationalite: 'Allemand', date_naissance: '1844-10-15'});

// ─── Nœuds Livre ─────────────────────────────────────────
MERGE (:Livre {livre_id: 1, titre: 'The Art of Computer Programming', isbn: '978-0-201-03801-0', annee_pub: 1968, editeur: 'Addison-Wesley', prix: 89.99, categorie_nom: 'Informatique'});
MERGE (:Livre {livre_id: 2, titre: 'Clean Code', isbn: '978-0-13-235088-4', annee_pub: 2008, editeur: 'Prentice Hall', prix: 45.0, categorie_nom: 'Informatique'});
MERGE (:Livre {livre_id: 3, titre: 'The Relational Model for DB', isbn: '978-0-201-14192-4', annee_pub: 1990, editeur: 'Addison-Wesley', prix: 55.0, categorie_nom: 'Informatique'});
MERGE (:Livre {livre_id: 4, titre: 'L\'Étranger', isbn: '978-2-07-036024-5', annee_pub: 1942, editeur: 'Gallimard', prix: 12.0, categorie_nom: 'Littérature'});
MERGE (:Livre {livre_id: 5, titre: 'Modern Operating Systems', isbn: '978-0-13-359162-0', annee_pub: 2014, editeur: 'Pearson', prix: 70.0, categorie_nom: 'Informatique'});
MERGE (:Livre {livre_id: 6, titre: 'Refactoring', isbn: '978-0-13-468599-1', annee_pub: 1999, editeur: 'Addison-Wesley', prix: 50.0, categorie_nom: 'Informatique'});
MERGE (:Livre {livre_id: 7, titre: 'Cent ans de solitude', isbn: '978-2-07-036088-7', annee_pub: 1967, editeur: 'Gallimard', prix: 14.0, categorie_nom: 'Littérature'});
MERGE (:Livre {livre_id: 8, titre: 'Ainsi parlait Zarathoustra', isbn: '978-2-07-041217-5', annee_pub: 1885, editeur: 'Gallimard', prix: 11.0, categorie_nom: 'Philosophie'});
MERGE (:Livre {livre_id: 9, titre: 'La Chute', isbn: '978-2-07-036026-9', annee_pub: 1956, editeur: 'Gallimard', prix: 10.0, categorie_nom: 'Littérature'});
MERGE (:Livre {livre_id: 10, titre: 'Patterns of Enterprise App Arch', isbn: '978-0-32-112521-7', annee_pub: 2002, editeur: 'Addison-Wesley', prix: 60.0, categorie_nom: 'Informatique'});

// ─── Nœuds Membre ────────────────────────────────────────
MERGE (:Membre {membre_id: 1, nom: 'Ben Ali', prenom: 'Mohamed', email: 'm.benali@email.com', telephone: '+216 71 000 001', adresse: 'Rue Habib Bourguiba 12', ville: 'Tunis', date_inscription: '2022-01-15'});
MERGE (:Membre {membre_id: 2, nom: 'Chaabane', prenom: 'Fatima', email: 'f.chaabane@email.com', telephone: '+216 71 000 002', adresse: 'Avenue Farhat Hached 5', ville: 'Sfax', date_inscription: '2022-03-20'});
MERGE (:Membre {membre_id: 3, nom: 'Trabelsi', prenom: 'Youssef', email: 'y.trabelsi@email.com', telephone: '+216 71 000 003', adresse: 'Rue de la Liberté 8', ville: 'Sousse', date_inscription: '2022-06-10'});
MERGE (:Membre {membre_id: 4, nom: 'Mansouri', prenom: 'Leila', email: 'l.mansouri@email.com', telephone: '+216 71 000 004', adresse: 'Avenue de Carthage 22', ville: 'Tunis', date_inscription: '2023-01-05'});
MERGE (:Membre {membre_id: 5, nom: 'Brahmi', prenom: 'Karim', email: 'k.brahmi@email.com', telephone: '+216 71 000 005', adresse: 'Rue Ibn Khaldoun 3', ville: 'Bizerte', date_inscription: '2023-04-18'});
MERGE (:Membre {membre_id: 6, nom: 'Sassi', prenom: 'Amira', email: 'a.sassi@email.com', telephone: '+216 71 000 006', adresse: 'Boulevard de l\'Indep 7', ville: 'Nabeul', date_inscription: '2023-07-22'});
MERGE (:Membre {membre_id: 7, nom: 'Mejri', prenom: 'Sofien', email: 's.mejri@email.com', telephone: '+216 71 000 007', adresse: 'Rue des Roses 15', ville: 'Tunis', date_inscription: '2023-09-30'});
MERGE (:Membre {membre_id: 8, nom: 'Khelifi', prenom: 'Nadia', email: 'n.khelifi@email.com', telephone: '+216 71 000 008', adresse: 'Avenue de l\'Armée 18', ville: 'Gabes', date_inscription: '2024-01-12'});

// ─── Nœuds Exemplaire ────────────────────────────────────
MERGE (:Exemplaire {exemplaire_id: 1, etat: 'bon', disponible: false});
MERGE (:Exemplaire {exemplaire_id: 2, etat: 'bon', disponible: true});
MERGE (:Exemplaire {exemplaire_id: 3, etat: 'use', disponible: false});
MERGE (:Exemplaire {exemplaire_id: 4, etat: 'bon', disponible: true});
MERGE (:Exemplaire {exemplaire_id: 5, etat: 'bon', disponible: false});
MERGE (:Exemplaire {exemplaire_id: 6, etat: 'bon', disponible: true});
MERGE (:Exemplaire {exemplaire_id: 7, etat: 'bon', disponible: true});
MERGE (:Exemplaire {exemplaire_id: 8, etat: 'bon', disponible: true});
MERGE (:Exemplaire {exemplaire_id: 9, etat: 'bon', disponible: false});
MERGE (:Exemplaire {exemplaire_id: 10, etat: 'bon', disponible: true});
MERGE (:Exemplaire {exemplaire_id: 11, etat: 'bon', disponible: true});
MERGE (:Exemplaire {exemplaire_id: 12, etat: 'bon', disponible: true});
MERGE (:Exemplaire {exemplaire_id: 13, etat: 'bon', disponible: true});
MERGE (:Exemplaire {exemplaire_id: 14, etat: 'bon', disponible: true});
MERGE (:Exemplaire {exemplaire_id: 15, etat: 'bon', disponible: true});
MERGE (:Exemplaire {exemplaire_id: 16, etat: 'bon', disponible: true});
MERGE (:Exemplaire {exemplaire_id: 17, etat: 'bon', disponible: true});
MERGE (:Exemplaire {exemplaire_id: 18, etat: 'bon', disponible: true});
MERGE (:Exemplaire {exemplaire_id: 19, etat: 'bon', disponible: true});
MERGE (:Exemplaire {exemplaire_id: 20, etat: 'bon', disponible: true});
// ════ 3. CRÉATION DES RELATIONS ══════════════════════════════════════

// ─── Relations A_ECRIT ───────────────────────────────────
MATCH (a:Auteur {auteur_id: 1}), (lv:Livre {livre_id: 1})
MERGE (a)-[:A_ECRIT]->(lv);
MATCH (a:Auteur {auteur_id: 2}), (lv:Livre {livre_id: 2})
MERGE (a)-[:A_ECRIT]->(lv);
MATCH (a:Auteur {auteur_id: 3}), (lv:Livre {livre_id: 3})
MERGE (a)-[:A_ECRIT]->(lv);
MATCH (a:Auteur {auteur_id: 4}), (lv:Livre {livre_id: 4})
MERGE (a)-[:A_ECRIT]->(lv);
MATCH (a:Auteur {auteur_id: 5}), (lv:Livre {livre_id: 5})
MERGE (a)-[:A_ECRIT]->(lv);
MATCH (a:Auteur {auteur_id: 6}), (lv:Livre {livre_id: 6})
MERGE (a)-[:A_ECRIT]->(lv);
MATCH (a:Auteur {auteur_id: 7}), (lv:Livre {livre_id: 7})
MERGE (a)-[:A_ECRIT]->(lv);
MATCH (a:Auteur {auteur_id: 8}), (lv:Livre {livre_id: 8})
MERGE (a)-[:A_ECRIT]->(lv);
MATCH (a:Auteur {auteur_id: 4}), (lv:Livre {livre_id: 9})
MERGE (a)-[:A_ECRIT]->(lv);
MATCH (a:Auteur {auteur_id: 6}), (lv:Livre {livre_id: 10})
MERGE (a)-[:A_ECRIT]->(lv);

// ─── Relations APPARTIENT_A ──────────────────────────────
MATCH (lv:Livre {livre_id: 1}), (c:Categorie {nom: 'Informatique'})
MERGE (lv)-[:APPARTIENT_A]->(c);
MATCH (lv:Livre {livre_id: 2}), (c:Categorie {nom: 'Informatique'})
MERGE (lv)-[:APPARTIENT_A]->(c);
MATCH (lv:Livre {livre_id: 3}), (c:Categorie {nom: 'Informatique'})
MERGE (lv)-[:APPARTIENT_A]->(c);
MATCH (lv:Livre {livre_id: 4}), (c:Categorie {nom: 'Littérature'})
MERGE (lv)-[:APPARTIENT_A]->(c);
MATCH (lv:Livre {livre_id: 5}), (c:Categorie {nom: 'Informatique'})
MERGE (lv)-[:APPARTIENT_A]->(c);
MATCH (lv:Livre {livre_id: 6}), (c:Categorie {nom: 'Informatique'})
MERGE (lv)-[:APPARTIENT_A]->(c);
MATCH (lv:Livre {livre_id: 7}), (c:Categorie {nom: 'Littérature'})
MERGE (lv)-[:APPARTIENT_A]->(c);
MATCH (lv:Livre {livre_id: 8}), (c:Categorie {nom: 'Philosophie'})
MERGE (lv)-[:APPARTIENT_A]->(c);
MATCH (lv:Livre {livre_id: 9}), (c:Categorie {nom: 'Littérature'})
MERGE (lv)-[:APPARTIENT_A]->(c);
MATCH (lv:Livre {livre_id: 10}), (c:Categorie {nom: 'Informatique'})
MERGE (lv)-[:APPARTIENT_A]->(c);

// ─── Relations A_EXEMPLAIRE ──────────────────────────────
MATCH (lv:Livre {livre_id: 1}), (ex:Exemplaire {exemplaire_id: 1})
MERGE (lv)-[:A_EXEMPLAIRE]->(ex);
MATCH (lv:Livre {livre_id: 1}), (ex:Exemplaire {exemplaire_id: 2})
MERGE (lv)-[:A_EXEMPLAIRE]->(ex);
MATCH (lv:Livre {livre_id: 2}), (ex:Exemplaire {exemplaire_id: 3})
MERGE (lv)-[:A_EXEMPLAIRE]->(ex);
MATCH (lv:Livre {livre_id: 2}), (ex:Exemplaire {exemplaire_id: 4})
MERGE (lv)-[:A_EXEMPLAIRE]->(ex);
MATCH (lv:Livre {livre_id: 3}), (ex:Exemplaire {exemplaire_id: 5})
MERGE (lv)-[:A_EXEMPLAIRE]->(ex);
MATCH (lv:Livre {livre_id: 3}), (ex:Exemplaire {exemplaire_id: 6})
MERGE (lv)-[:A_EXEMPLAIRE]->(ex);
MATCH (lv:Livre {livre_id: 4}), (ex:Exemplaire {exemplaire_id: 7})
MERGE (lv)-[:A_EXEMPLAIRE]->(ex);
MATCH (lv:Livre {livre_id: 4}), (ex:Exemplaire {exemplaire_id: 8})
MERGE (lv)-[:A_EXEMPLAIRE]->(ex);
MATCH (lv:Livre {livre_id: 5}), (ex:Exemplaire {exemplaire_id: 9})
MERGE (lv)-[:A_EXEMPLAIRE]->(ex);
MATCH (lv:Livre {livre_id: 5}), (ex:Exemplaire {exemplaire_id: 10})
MERGE (lv)-[:A_EXEMPLAIRE]->(ex);
MATCH (lv:Livre {livre_id: 6}), (ex:Exemplaire {exemplaire_id: 11})
MERGE (lv)-[:A_EXEMPLAIRE]->(ex);
MATCH (lv:Livre {livre_id: 6}), (ex:Exemplaire {exemplaire_id: 12})
MERGE (lv)-[:A_EXEMPLAIRE]->(ex);
MATCH (lv:Livre {livre_id: 7}), (ex:Exemplaire {exemplaire_id: 13})
MERGE (lv)-[:A_EXEMPLAIRE]->(ex);
MATCH (lv:Livre {livre_id: 7}), (ex:Exemplaire {exemplaire_id: 14})
MERGE (lv)-[:A_EXEMPLAIRE]->(ex);
MATCH (lv:Livre {livre_id: 8}), (ex:Exemplaire {exemplaire_id: 15})
MERGE (lv)-[:A_EXEMPLAIRE]->(ex);
MATCH (lv:Livre {livre_id: 8}), (ex:Exemplaire {exemplaire_id: 16})
MERGE (lv)-[:A_EXEMPLAIRE]->(ex);
MATCH (lv:Livre {livre_id: 9}), (ex:Exemplaire {exemplaire_id: 17})
MERGE (lv)-[:A_EXEMPLAIRE]->(ex);
MATCH (lv:Livre {livre_id: 9}), (ex:Exemplaire {exemplaire_id: 18})
MERGE (lv)-[:A_EXEMPLAIRE]->(ex);
MATCH (lv:Livre {livre_id: 10}), (ex:Exemplaire {exemplaire_id: 19})
MERGE (lv)-[:A_EXEMPLAIRE]->(ex);
MATCH (lv:Livre {livre_id: 10}), (ex:Exemplaire {exemplaire_id: 20})
MERGE (lv)-[:A_EXEMPLAIRE]->(ex);

// ─── Relations A_EMPRUNTE (avec propriétés) ──────────────
MATCH (m:Membre {membre_id: 1}), (ex:Exemplaire {exemplaire_id: 1})
MERGE (m)-[:A_EMPRUNTE {emprunt_id: 1, date_emprunt: '2024-03-01', date_retour_prevue: '2024-03-15', date_retour_reelle: '2024-03-14', statut: 'rendu'}]->(ex);
MATCH (m:Membre {membre_id: 2}), (ex:Exemplaire {exemplaire_id: 3})
MERGE (m)-[:A_EMPRUNTE {emprunt_id: 2, date_emprunt: '2024-03-05', date_retour_prevue: '2024-03-19', date_retour_reelle: null, statut: 'en_cours'}]->(ex);
MATCH (m:Membre {membre_id: 3}), (ex:Exemplaire {exemplaire_id: 5})
MERGE (m)-[:A_EMPRUNTE {emprunt_id: 3, date_emprunt: '2024-02-20', date_retour_prevue: '2024-03-05', date_retour_reelle: null, statut: 'retard'}]->(ex);
MATCH (m:Membre {membre_id: 4}), (ex:Exemplaire {exemplaire_id: 9})
MERGE (m)-[:A_EMPRUNTE {emprunt_id: 4, date_emprunt: '2024-03-10', date_retour_prevue: '2024-03-24', date_retour_reelle: null, statut: 'en_cours'}]->(ex);
MATCH (m:Membre {membre_id: 5}), (ex:Exemplaire {exemplaire_id: 2})
MERGE (m)-[:A_EMPRUNTE {emprunt_id: 5, date_emprunt: '2024-03-12', date_retour_prevue: '2024-03-26', date_retour_reelle: '2024-03-25', statut: 'rendu'}]->(ex);
MATCH (m:Membre {membre_id: 6}), (ex:Exemplaire {exemplaire_id: 4})
MERGE (m)-[:A_EMPRUNTE {emprunt_id: 6, date_emprunt: '2024-02-28', date_retour_prevue: '2024-03-13', date_retour_reelle: null, statut: 'retard'}]->(ex);
MATCH (m:Membre {membre_id: 7}), (ex:Exemplaire {exemplaire_id: 6})
MERGE (m)-[:A_EMPRUNTE {emprunt_id: 7, date_emprunt: '2024-03-15', date_retour_prevue: '2024-03-29', date_retour_reelle: null, statut: 'en_cours'}]->(ex);
MATCH (m:Membre {membre_id: 8}), (ex:Exemplaire {exemplaire_id: 8})
MERGE (m)-[:A_EMPRUNTE {emprunt_id: 8, date_emprunt: '2024-03-18', date_retour_prevue: '2024-04-01', date_retour_reelle: null, statut: 'en_cours'}]->(ex);
// ════ 4. REQUÊTES ANALYTIQUES ════════════════════════════════════════

// ── G1_livres_par_categorie ──

// G1 : Livres d'une catégorie (traversée de relation)
MATCH (l:Livre)-[:APPARTIENT_A]->(c:Categorie {nom: 'Informatique'})
MATCH (a:Auteur)-[:A_ECRIT]->(l)
RETURN l.titre AS titre,
       a.nom + ' ' + a.prenom AS auteur,
       l.prix AS prix
ORDER BY l.titre;


// ── G2_livres_auteur ──

// G2 : Livres d'un auteur avec sa catégorie
MATCH (a:Auteur {nom: 'Fowler'})-[:A_ECRIT]->(l:Livre)-[:APPARTIENT_A]->(c:Categorie)
RETURN l.titre AS titre, l.annee_pub AS annee, c.nom AS categorie
ORDER BY l.annee_pub DESC;


// ── G3_emprunts_en_cours ──

// G3 : Tous les emprunts actifs (chemin membre → exemplaire → livre)
MATCH (m:Membre)-[e:A_EMPRUNTE]->(ex:Exemplaire)<-[:A_EXEMPLAIRE]-(l:Livre)
WHERE e.statut IN ['en_cours', 'retard']
RETURN m.prenom + ' ' + m.nom AS membre,
       l.titre AS livre,
       e.date_emprunt AS depuis,
       e.statut AS statut
ORDER BY e.date_emprunt;


// ── G4_membres_ville ──

// G4 : Membres d'une ville
MATCH (m:Membre {ville: 'Tunis'})
RETURN m.nom AS nom, m.prenom AS prenom, m.email AS email
ORDER BY m.nom;


// ── G5_retards ──

// G5 : Emprunts en retard avec calcul du chemin complet
MATCH (m:Membre)-[e:A_EMPRUNTE]->(ex:Exemplaire)<-[:A_EXEMPLAIRE]-(l:Livre)
WHERE e.statut = 'retard'
RETURN m.prenom + ' ' + m.nom AS membre,
       l.titre AS livre,
       e.date_retour_prevue AS prevue,
       m.email AS contact
ORDER BY e.date_retour_prevue;


// ── G6_recommandations ──

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


// ── G7_chemin_plus_court ──

// G7 : Chemin le plus court entre deux membres (via livres communs)
// Utilise l'algorithme de plus court chemin de Neo4j
MATCH path = shortestPath(
    (m1:Membre {nom: 'Ben Ali'})-[*..6]-(m2:Membre {nom: 'Chaabane'})
)
RETURN [node IN nodes(path) | coalesce(node.titre, node.nom, node.nom)] AS chemin,
       length(path) AS distance;


// ── G8_centralite_livres ──

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


// ── G9_membres_connectes ──

// G9 : Réseau de lecture — membres connectés par livres communs
MATCH (m1:Membre)-[:A_EMPRUNTE]->(:Exemplaire)<-[:A_EXEMPLAIRE]-(l:Livre)
      <-[:A_EXEMPLAIRE]-(:Exemplaire)<-[:A_EMPRUNTE]-(m2:Membre)
WHERE m1 <> m2 AND id(m1) < id(m2)
RETURN m1.prenom + ' ' + m1.nom AS membre1,
       m2.prenom + ' ' + m2.nom AS membre2,
       collect(DISTINCT l.titre) AS livres_communs,
       count(DISTINCT l) AS nb_livres_communs
ORDER BY nb_livres_communs DESC;


// ── G10_analyse_categories ──

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
