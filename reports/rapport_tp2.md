## Question 1.b:

![simple](tp2.1.png)

## Question 1.c : Structure de données

![simple](tp2.2.png)

![simple](tp2.3.png)

## Question 2.a : 

Le schéma a été créé .

## Question 2.c:

![simple](tp2.4.png)

## Question 2.d:

![simple](tp2.5.png)

### Commentaires

* **labels** : contient la variable cible indiquant si un utilisateur a churné ou non.
* **payments_agg_90d** : regroupe les informations de paiements échoués d'un utilisateur sur les 90 derniers jours.
* **subscriptions** : décrit les caractéristiques du contrat et de l'abonnement de chaque utilisateur.
* **support_agg_90d** : regroupe les données relatives aux tickets support d'un utilisateur sur les 90 derniers jours.
* **usage_agg_30d** : contient les métriques d'utilisation du service par utilisateur sur les 30 derniers jours.
* **users** : recense les informations démographiques et générales de chaque utilisateur.

## Question3.a : Rôle du conteneur Prefect

Le conteneur Prefect sert de moteur d'orchestration du pipeline d’ingestion : il planifie, exécute et surveille les flux d’ingestion de données, assurant leur automatisation et leur fiabilité au sein de l’architecture. Il gére aussi les logs, les erreurs , les états et les indépendances.

## Question 3.b: Logique de la fonction upsert_csv

La fonction `upsert_csv` charge un fichier CSV dans une table Postgres en suivant une stratégie d'upsert. Elle crée d'abord une table temporaire pour stocker les données du CSV, convertit certaines colonnes si nécessaire (dates, booléens), puis insère les données dans la table cible en utilisant `INSERT ... ON CONFLICT ... DO UPDATE` pour mettre à jour les lignes existantes selon les clés primaires. Enfin, la table temporaire est supprimée.

## Question 3.c:

![simple](tp2.6.png)

Après l'ingestion de month_000, nous avons 7043 clients dans la base.

Voici un texte que vous pouvez ajouter à votre rapport :



## Question 4.a:

La fonction **`validate_with_ge`** sert à valider la qualité des données après leur ingestion dans la base PostgreSQL.
Pour chaque table, elle utilise Great Expectations afin de vérifier que les colonnes attendues sont présentes et que certaines valeurs numériques respectent des bornes logiques (par exemple, ≥ 0 pour les agrégats d’usage).
Si une expectation échoue, la fonction lève une exception, ce qui fait échouer le flow Prefect, garantissant ainsi que seules des données conformes sont conservées dans le pipeline.

## Question 4.b:

![simple](tp2.7.png)

## Validation des données

Pour la table `usage_agg_30d`, j'ai défini les expectations suivantes :

```python
# Vérifie que le nombre d'heures regardées sur 30 jours est positif
gdf.expect_column_values_to_be_between("watch_hours_30d", min_value=0)

# Vérifie que la durée moyenne des sessions sur 7 jours est positive
gdf.expect_column_values_to_be_between("avg_session_mins_7d", min_value=0)
```

### Choix des bornes

* **watch_hours_30d >= 0** : un utilisateur ne peut pas avoir regardé un nombre d’heures négatif.
* **avg_session_mins_7d >= 0** : la durée moyenne d’une session ne peut pas être négative.

Ces bornes permettent de protéger le modèle en excluant des valeurs impossibles ou aberrantes. Elles servent également à détecter des fichiers d’export corrompus ou des erreurs d’ingestion, garantissant que seules des données cohérentes et fiables sont utilisées pour l’entraînement et l’évaluation du modèle de churn.

## Question 5.b:

![simple](tp2.8.png)


**Commentaire** :
Pour le snapshot du 31 janvier 2024, nous avons bien **7043 lignes**, ce qui correspond exactement au nombre de clients présents après `month_000`.
Pour le snapshot du 29 février 2024, nous avons **0 ligne** car aucune ingestion de données n’a encore été effectuée pour le mois de février.
Autrement dit, le snapshot ne crée pas automatiquement de nouvelles lignes pour des mois futurs ; il ne contient que les données effectivement ingérées à la date `as_of` spécifiée. Cela permet de conserver un historique fidèle et idempotent des métriques par mois.

## Synthèse du TP2

### Schéma du pipeline d’ingestion (ASCII)

```
CSV month_000 --> Prefect flow --> Upsert dans tables principales
                                   |
                                   v
                        Validate_with_GE (qualité des données)
                                   |
                                   v
                         Snapshot tables par as_of
```

### Explication

* Nous ne travaillons pas directement sur les tables live pour entraîner un modèle afin de préserver l’intégrité et la stabilité des données. Les tables live peuvent évoluer et contenir des mises à jour partielles ou des erreurs temporaires.
* Les snapshots sont importants pour éviter la data leakage et pour garantir la reproductibilité temporelle : chaque snapshot capture l’état exact des métriques d’un mois donné, permettant de reconstruire exactement l’historique utilisé pour entraîner ou tester un modèle.

### Réflexion personnelle

Le point le plus difficile dans la mise en place de l’ingestion a été de s’assurer que les données étaient correctement typées et cohérentes avant insertion, notamment pour les colonnes booléennes et numériques. J’ai rencontré plusieurs erreurs liées aux chemins de fichiers Docker et aux services non démarrés, que j’ai corrigées en vérifiant que les conteneurs Prefect et Postgres étaient bien en fonctionnement avant d’exécuter les flows.
