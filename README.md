# projet_medaillon_et_graphe_de_connaissances

Ce projet implémente une pipeline de données complète suivant l'architecture médaillon (Bronze/Silver/Gold) pour ensuite ingérer et traiter un graphe de connaissances (Knowledge Graph) à grande échelle.



## Technologies du projet :

- Conteneurisation Docker :
    - PostgreSQL : Metastore (base de données interne d'Airflow)
    - Apache/Airflow
    - Neo4j_db (base de données orientée graphe)

- Apache Airflow : permet l'automatisation et le monitoring de la pipeline
- API FastAPI



## Lancement du projet :

### Configuration de l'environnement Docker avec un docker compose

Créer un docker compose afin de créer les images :
```
docker compose -f docker-compose.yaml --env-file .env.dev build
docker compose -f docker-compose.yaml --env-file .env.dev up -d
```

### Initialisation de l'enironnement Airflow

```
docker compose -f docker-compose.yaml --env-file .env.dev run airflow db init
```

### Création de l'utilisateur admin d'Airflow

```
docker compose -f docker-compose.yaml --env-file .env.dev run airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

docker compose -f docker-compose.yaml --env-file .env.dev up -d
```

### Accès à l'interface d'Airflow dans le navigateur de l'utilisateur

- Le projet est à présent lancé, l'interface d'Airflow est disponible à l'adresse suivante: \
http://localhost:8080/home

- Visualisation des DAGs (Directed Acyclic Graph) : pipelines composés de tasks
