-- ════════════════════════════════════════════════════════
-- SCHÉMA CASSANDRA — Bibliothèque
-- ════════════════════════════════════════════════════════

CREATE KEYSPACE IF NOT EXISTS bibliotheque
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 3
}
AND DURABLE_WRITES = true;

USE bibliotheque;


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


-- Q4 : "Quels exemplaires du livre X sont disponibles ?"
CREATE TABLE IF NOT EXISTS disponibilite_livres (
    livre_id         INT,
    titre            TEXT,
    exemplaire_id    INT,
    etat             TEXT,
    disponible       BOOLEAN,
    PRIMARY KEY ((livre_id), exemplaire_id)
);


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


-- ════════ INSERT ════════

INSERT INTO livres_par_categorie (categorie, titre, livre_id, isbn, annee_pub, editeur, prix, auteur_nom, auteur_prenom, auteur_nationalite, nb_exemplaires, nb_disponibles) VALUES ('Informatique', 'The Art of Computer Programming', 1, '978-0-201-03801-0', 1968, 'Addison-Wesley', 89.99, 'Knuth', 'Donald', 'Américain', 2, 1);
INSERT INTO livres_par_auteur (auteur_nom, auteur_prenom, titre, livre_id, isbn, annee_pub, editeur, prix, categorie, nb_disponibles) VALUES ('Knuth', 'Donald', 'The Art of Computer Programming', 1, '978-0-201-03801-0', 1968, 'Addison-Wesley', 89.99, 'Informatique', 1);
INSERT INTO livres_par_categorie (categorie, titre, livre_id, isbn, annee_pub, editeur, prix, auteur_nom, auteur_prenom, auteur_nationalite, nb_exemplaires, nb_disponibles) VALUES ('Informatique', 'Clean Code', 2, '978-0-13-235088-4', 2008, 'Prentice Hall', 45.0, 'Martin', 'Robert', 'Américain', 2, 1);
INSERT INTO livres_par_auteur (auteur_nom, auteur_prenom, titre, livre_id, isbn, annee_pub, editeur, prix, categorie, nb_disponibles) VALUES ('Martin', 'Robert', 'Clean Code', 2, '978-0-13-235088-4', 2008, 'Prentice Hall', 45.0, 'Informatique', 1);
INSERT INTO livres_par_categorie (categorie, titre, livre_id, isbn, annee_pub, editeur, prix, auteur_nom, auteur_prenom, auteur_nationalite, nb_exemplaires, nb_disponibles) VALUES ('Informatique', 'The Relational Model for DB', 3, '978-0-201-14192-4', 1990, 'Addison-Wesley', 55.0, 'Codd', 'Edgar', 'Britannique', 2, 1);
INSERT INTO livres_par_auteur (auteur_nom, auteur_prenom, titre, livre_id, isbn, annee_pub, editeur, prix, categorie, nb_disponibles) VALUES ('Codd', 'Edgar', 'The Relational Model for DB', 3, '978-0-201-14192-4', 1990, 'Addison-Wesley', 55.0, 'Informatique', 1);
INSERT INTO livres_par_categorie (categorie, titre, livre_id, isbn, annee_pub, editeur, prix, auteur_nom, auteur_prenom, auteur_nationalite, nb_exemplaires, nb_disponibles) VALUES ('Littérature', 'L''Étranger', 4, '978-2-07-036024-5', 1942, 'Gallimard', 12.0, 'Camus', 'Albert', 'Français', 2, 2);
INSERT INTO livres_par_auteur (auteur_nom, auteur_prenom, titre, livre_id, isbn, annee_pub, editeur, prix, categorie, nb_disponibles) VALUES ('Camus', 'Albert', 'L''Étranger', 4, '978-2-07-036024-5', 1942, 'Gallimard', 12.0, 'Littérature', 2);
INSERT INTO livres_par_categorie (categorie, titre, livre_id, isbn, annee_pub, editeur, prix, auteur_nom, auteur_prenom, auteur_nationalite, nb_exemplaires, nb_disponibles) VALUES ('Informatique', 'Modern Operating Systems', 5, '978-0-13-359162-0', 2014, 'Pearson', 70.0, 'Tanenbaum', 'Andrew', 'Américain', 2, 1);
INSERT INTO livres_par_auteur (auteur_nom, auteur_prenom, titre, livre_id, isbn, annee_pub, editeur, prix, categorie, nb_disponibles) VALUES ('Tanenbaum', 'Andrew', 'Modern Operating Systems', 5, '978-0-13-359162-0', 2014, 'Pearson', 70.0, 'Informatique', 1);
INSERT INTO livres_par_categorie (categorie, titre, livre_id, isbn, annee_pub, editeur, prix, auteur_nom, auteur_prenom, auteur_nationalite, nb_exemplaires, nb_disponibles) VALUES ('Informatique', 'Refactoring', 6, '978-0-13-468599-1', 1999, 'Addison-Wesley', 50.0, 'Fowler', 'Martin', 'Britannique', 2, 2);
INSERT INTO livres_par_auteur (auteur_nom, auteur_prenom, titre, livre_id, isbn, annee_pub, editeur, prix, categorie, nb_disponibles) VALUES ('Fowler', 'Martin', 'Refactoring', 6, '978-0-13-468599-1', 1999, 'Addison-Wesley', 50.0, 'Informatique', 2);
INSERT INTO livres_par_categorie (categorie, titre, livre_id, isbn, annee_pub, editeur, prix, auteur_nom, auteur_prenom, auteur_nationalite, nb_exemplaires, nb_disponibles) VALUES ('Littérature', 'Cent ans de solitude', 7, '978-2-07-036088-7', 1967, 'Gallimard', 14.0, 'García M.', 'Gabriel', 'Colombien', 2, 2);
INSERT INTO livres_par_auteur (auteur_nom, auteur_prenom, titre, livre_id, isbn, annee_pub, editeur, prix, categorie, nb_disponibles) VALUES ('García M.', 'Gabriel', 'Cent ans de solitude', 7, '978-2-07-036088-7', 1967, 'Gallimard', 14.0, 'Littérature', 2);
INSERT INTO livres_par_categorie (categorie, titre, livre_id, isbn, annee_pub, editeur, prix, auteur_nom, auteur_prenom, auteur_nationalite, nb_exemplaires, nb_disponibles) VALUES ('Philosophie', 'Ainsi parlait Zarathoustra', 8, '978-2-07-041217-5', 1885, 'Gallimard', 11.0, 'Nietzsche', 'Friedrich', 'Allemand', 2, 2);
INSERT INTO livres_par_auteur (auteur_nom, auteur_prenom, titre, livre_id, isbn, annee_pub, editeur, prix, categorie, nb_disponibles) VALUES ('Nietzsche', 'Friedrich', 'Ainsi parlait Zarathoustra', 8, '978-2-07-041217-5', 1885, 'Gallimard', 11.0, 'Philosophie', 2);
INSERT INTO livres_par_categorie (categorie, titre, livre_id, isbn, annee_pub, editeur, prix, auteur_nom, auteur_prenom, auteur_nationalite, nb_exemplaires, nb_disponibles) VALUES ('Littérature', 'La Chute', 9, '978-2-07-036026-9', 1956, 'Gallimard', 10.0, 'Camus', 'Albert', 'Français', 2, 2);
INSERT INTO livres_par_auteur (auteur_nom, auteur_prenom, titre, livre_id, isbn, annee_pub, editeur, prix, categorie, nb_disponibles) VALUES ('Camus', 'Albert', 'La Chute', 9, '978-2-07-036026-9', 1956, 'Gallimard', 10.0, 'Littérature', 2);
INSERT INTO livres_par_categorie (categorie, titre, livre_id, isbn, annee_pub, editeur, prix, auteur_nom, auteur_prenom, auteur_nationalite, nb_exemplaires, nb_disponibles) VALUES ('Informatique', 'Patterns of Enterprise App Arch', 10, '978-0-32-112521-7', 2002, 'Addison-Wesley', 60.0, 'Fowler', 'Martin', 'Britannique', 2, 2);
INSERT INTO livres_par_auteur (auteur_nom, auteur_prenom, titre, livre_id, isbn, annee_pub, editeur, prix, categorie, nb_disponibles) VALUES ('Fowler', 'Martin', 'Patterns of Enterprise App Arch', 10, '978-0-32-112521-7', 2002, 'Addison-Wesley', 60.0, 'Informatique', 2);
INSERT INTO membres_par_ville (ville, nom, prenom, membre_id, email, telephone, adresse, date_inscription) VALUES ('Tunis', 'Ben Ali', 'Mohamed', 1, 'm.benali@email.com', '+216 71 000 001', 'Rue Habib Bourguiba 12', '2022-01-15');
INSERT INTO membres_par_ville (ville, nom, prenom, membre_id, email, telephone, adresse, date_inscription) VALUES ('Sfax', 'Chaabane', 'Fatima', 2, 'f.chaabane@email.com', '+216 71 000 002', 'Avenue Farhat Hached 5', '2022-03-20');
INSERT INTO membres_par_ville (ville, nom, prenom, membre_id, email, telephone, adresse, date_inscription) VALUES ('Sousse', 'Trabelsi', 'Youssef', 3, 'y.trabelsi@email.com', '+216 71 000 003', 'Rue de la Liberté 8', '2022-06-10');
INSERT INTO membres_par_ville (ville, nom, prenom, membre_id, email, telephone, adresse, date_inscription) VALUES ('Tunis', 'Mansouri', 'Leila', 4, 'l.mansouri@email.com', '+216 71 000 004', 'Avenue de Carthage 22', '2023-01-05');
INSERT INTO membres_par_ville (ville, nom, prenom, membre_id, email, telephone, adresse, date_inscription) VALUES ('Bizerte', 'Brahmi', 'Karim', 5, 'k.brahmi@email.com', '+216 71 000 005', 'Rue Ibn Khaldoun 3', '2023-04-18');
INSERT INTO membres_par_ville (ville, nom, prenom, membre_id, email, telephone, adresse, date_inscription) VALUES ('Nabeul', 'Sassi', 'Amira', 6, 'a.sassi@email.com', '+216 71 000 006', 'Boulevard de l'Indep 7', '2023-07-22');
INSERT INTO membres_par_ville (ville, nom, prenom, membre_id, email, telephone, adresse, date_inscription) VALUES ('Tunis', 'Mejri', 'Sofien', 7, 's.mejri@email.com', '+216 71 000 007', 'Rue des Roses 15', '2023-09-30');
INSERT INTO membres_par_ville (ville, nom, prenom, membre_id, email, telephone, adresse, date_inscription) VALUES ('Gabes', 'Khelifi', 'Nadia', 8, 'n.khelifi@email.com', '+216 71 000 008', 'Avenue de l'Armée 18', '2024-01-12');
INSERT INTO disponibilite_livres (livre_id, titre, exemplaire_id, etat, disponible) VALUES (1, 'The Art of Computer Programming', 1, 'bon', false);
INSERT INTO disponibilite_livres (livre_id, titre, exemplaire_id, etat, disponible) VALUES (1, 'The Art of Computer Programming', 2, 'bon', true);
INSERT INTO disponibilite_livres (livre_id, titre, exemplaire_id, etat, disponible) VALUES (2, 'Clean Code', 3, 'use', false);
INSERT INTO disponibilite_livres (livre_id, titre, exemplaire_id, etat, disponible) VALUES (2, 'Clean Code', 4, 'bon', true);
INSERT INTO disponibilite_livres (livre_id, titre, exemplaire_id, etat, disponible) VALUES (3, 'The Relational Model for DB', 5, 'bon', false);
INSERT INTO disponibilite_livres (livre_id, titre, exemplaire_id, etat, disponible) VALUES (3, 'The Relational Model for DB', 6, 'bon', true);
INSERT INTO disponibilite_livres (livre_id, titre, exemplaire_id, etat, disponible) VALUES (4, 'L''Étranger', 7, 'bon', true);
INSERT INTO disponibilite_livres (livre_id, titre, exemplaire_id, etat, disponible) VALUES (4, 'L''Étranger', 8, 'bon', true);
INSERT INTO disponibilite_livres (livre_id, titre, exemplaire_id, etat, disponible) VALUES (5, 'Modern Operating Systems', 9, 'bon', false);
INSERT INTO disponibilite_livres (livre_id, titre, exemplaire_id, etat, disponible) VALUES (5, 'Modern Operating Systems', 10, 'bon', true);
INSERT INTO disponibilite_livres (livre_id, titre, exemplaire_id, etat, disponible) VALUES (6, 'Refactoring', 11, 'bon', true);
INSERT INTO disponibilite_livres (livre_id, titre, exemplaire_id, etat, disponible) VALUES (6, 'Refactoring', 12, 'bon', true);
INSERT INTO disponibilite_livres (livre_id, titre, exemplaire_id, etat, disponible) VALUES (7, 'Cent ans de solitude', 13, 'bon', true);
INSERT INTO disponibilite_livres (livre_id, titre, exemplaire_id, etat, disponible) VALUES (7, 'Cent ans de solitude', 14, 'bon', true);
INSERT INTO disponibilite_livres (livre_id, titre, exemplaire_id, etat, disponible) VALUES (8, 'Ainsi parlait Zarathoustra', 15, 'bon', true);
INSERT INTO disponibilite_livres (livre_id, titre, exemplaire_id, etat, disponible) VALUES (8, 'Ainsi parlait Zarathoustra', 16, 'bon', true);
INSERT INTO disponibilite_livres (livre_id, titre, exemplaire_id, etat, disponible) VALUES (9, 'La Chute', 17, 'bon', true);
INSERT INTO disponibilite_livres (livre_id, titre, exemplaire_id, etat, disponible) VALUES (9, 'La Chute', 18, 'bon', true);
INSERT INTO disponibilite_livres (livre_id, titre, exemplaire_id, etat, disponible) VALUES (10, 'Patterns of Enterprise App Arch', 19, 'bon', true);
INSERT INTO disponibilite_livres (livre_id, titre, exemplaire_id, etat, disponible) VALUES (10, 'Patterns of Enterprise App Arch', 20, 'bon', true);
INSERT INTO emprunts_par_membre (membre_id, membre_email, date_emprunt, emprunt_id, livre_titre, livre_isbn, exemplaire_id, date_retour_prevue, date_retour_reelle, statut) VALUES (1, 'm.benali@email.com', '2024-03-01', 1, 'The Art of Computer Programming', '978-0-201-03801-0', 1, '2024-03-15', '2024-03-14', 'rendu');
INSERT INTO emprunts_par_membre (membre_id, membre_email, date_emprunt, emprunt_id, livre_titre, livre_isbn, exemplaire_id, date_retour_prevue, date_retour_reelle, statut) VALUES (2, 'f.chaabane@email.com', '2024-03-05', 2, 'Clean Code', '978-0-13-235088-4', 3, '2024-03-19', null, 'en_cours');
INSERT INTO emprunts_en_retard (statut, date_retour_prevue, emprunt_id, membre_nom, membre_prenom, membre_email, livre_titre, date_emprunt) VALUES ('en_cours', '2024-03-19', 2, 'Chaabane', 'Fatima', 'f.chaabane@email.com', 'Clean Code', '2024-03-05');
INSERT INTO emprunts_par_membre (membre_id, membre_email, date_emprunt, emprunt_id, livre_titre, livre_isbn, exemplaire_id, date_retour_prevue, date_retour_reelle, statut) VALUES (3, 'y.trabelsi@email.com', '2024-02-20', 3, 'The Relational Model for DB', '978-0-201-14192-4', 5, '2024-03-05', null, 'retard');
INSERT INTO emprunts_en_retard (statut, date_retour_prevue, emprunt_id, membre_nom, membre_prenom, membre_email, livre_titre, date_emprunt) VALUES ('retard', '2024-03-05', 3, 'Trabelsi', 'Youssef', 'y.trabelsi@email.com', 'The Relational Model for DB', '2024-02-20');
INSERT INTO emprunts_par_membre (membre_id, membre_email, date_emprunt, emprunt_id, livre_titre, livre_isbn, exemplaire_id, date_retour_prevue, date_retour_reelle, statut) VALUES (4, 'l.mansouri@email.com', '2024-03-10', 4, 'Modern Operating Systems', '978-0-13-359162-0', 9, '2024-03-24', null, 'en_cours');
INSERT INTO emprunts_en_retard (statut, date_retour_prevue, emprunt_id, membre_nom, membre_prenom, membre_email, livre_titre, date_emprunt) VALUES ('en_cours', '2024-03-24', 4, 'Mansouri', 'Leila', 'l.mansouri@email.com', 'Modern Operating Systems', '2024-03-10');
INSERT INTO emprunts_par_membre (membre_id, membre_email, date_emprunt, emprunt_id, livre_titre, livre_isbn, exemplaire_id, date_retour_prevue, date_retour_reelle, statut) VALUES (5, 'k.brahmi@email.com', '2024-03-12', 5, 'The Art of Computer Programming', '978-0-201-03801-0', 2, '2024-03-26', '2024-03-25', 'rendu');
INSERT INTO emprunts_par_membre (membre_id, membre_email, date_emprunt, emprunt_id, livre_titre, livre_isbn, exemplaire_id, date_retour_prevue, date_retour_reelle, statut) VALUES (6, 'a.sassi@email.com', '2024-02-28', 6, 'Clean Code', '978-0-13-235088-4', 4, '2024-03-13', null, 'retard');
INSERT INTO emprunts_en_retard (statut, date_retour_prevue, emprunt_id, membre_nom, membre_prenom, membre_email, livre_titre, date_emprunt) VALUES ('retard', '2024-03-13', 6, 'Sassi', 'Amira', 'a.sassi@email.com', 'Clean Code', '2024-02-28');
INSERT INTO emprunts_par_membre (membre_id, membre_email, date_emprunt, emprunt_id, livre_titre, livre_isbn, exemplaire_id, date_retour_prevue, date_retour_reelle, statut) VALUES (7, 's.mejri@email.com', '2024-03-15', 7, 'The Relational Model for DB', '978-0-201-14192-4', 6, '2024-03-29', null, 'en_cours');
INSERT INTO emprunts_en_retard (statut, date_retour_prevue, emprunt_id, membre_nom, membre_prenom, membre_email, livre_titre, date_emprunt) VALUES ('en_cours', '2024-03-29', 7, 'Mejri', 'Sofien', 's.mejri@email.com', 'The Relational Model for DB', '2024-03-15');
INSERT INTO emprunts_par_membre (membre_id, membre_email, date_emprunt, emprunt_id, livre_titre, livre_isbn, exemplaire_id, date_retour_prevue, date_retour_reelle, statut) VALUES (8, 'n.khelifi@email.com', '2024-03-18', 8, 'L''Étranger', '978-2-07-036024-5', 8, '2024-04-01', null, 'en_cours');
INSERT INTO emprunts_en_retard (statut, date_retour_prevue, emprunt_id, membre_nom, membre_prenom, membre_email, livre_titre, date_emprunt) VALUES ('en_cours', '2024-04-01', 8, 'Khelifi', 'Nadia', 'n.khelifi@email.com', 'L''Étranger', '2024-03-18');