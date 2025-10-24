import pandas as pd
import glob
import os
from neo4j import GraphDatabase


def preparer_fichiers_CSV(rep_silver_path, rep_gold_path):

    """
    1ère partie : concatène plusieurs fichiers Parquet des partitions de edges en un seul fichier CSV et renomme également certaines colonnes des dataframe 'nodes' et 'edges'.
    """ 

    dfs = []

    for edges_shard_path in sorted(glob.glob("../data/silver/shard=*/edges.parquet")):
        print(f"Lecture : {edges_shard_path}")
        df = pd.read_parquet(edges_shard_path)
        dfs.append(df)

    merged = pd.concat(dfs, ignore_index=True)
    df_edges_cleaned = merged.copy()
    df_edges_cleaned = df_edges_cleaned.rename(columns={"src":":START_ID", "dst":":END_ID"})    

    # Crée le dossier s'il n'existe pas
    os.makedirs(rep_gold_path, exist_ok=True)

    edges_csv_path = os.path.join(rep_gold_path, "edges.csv")

    edges_file_name  = edges_csv_path.split("/")[-1]

    # Sauvegarde des partitions edges concaténés dans un seul fichier CSV
    df_edges_cleaned.to_csv(edges_csv_path, index=False)
    print(f"Fusion terminée : {edges_file_name} ({len(df_edges_cleaned)} lignes)")    

    cleaned_nodes_parquet_path = os.path.join(rep_silver_path, "nodes.parquet")

    nodes_file_name  = cleaned_nodes_parquet_path.split("/")[-1]

    try:
        df_nodes_cleaned = pd.read_parquet(cleaned_nodes_parquet_path)
        print(f"\nFichier '{nodes_file_name}' chargé : {len(df_nodes_cleaned)} lignes.")
    except FileNotFoundError:
        print(f"Erreur : Le fichier '{nodes_file_name}' n'a pas été trouvé.")

    df_nodes_cleaned = df_nodes_cleaned.rename(columns={"id":"id:ID"})

    nodes_csv_path = os.path.join(rep_gold_path, "nodes.csv")

    nodes_csv_file_name  = nodes_csv_path.split("/")[-1]

    # Sauvegarde des nodes dans un fichier CSV
    df_nodes_cleaned.to_csv(nodes_csv_path, index=False)
    print(f"Fusion terminée : {nodes_csv_file_name} ({len(df_nodes_cleaned)} lignes)")


def inserer_CSV_dans_Neo4j(rep_gold_path):

    """
    2ème partie : insère les 2 fichiers CSV 'nodes' et 'edges' dans Neo4j.
    """

    nodes_csv_path = os.path.join(rep_gold_path, "nodes.csv")
    edges_csv_path = os.path.join(rep_gold_path, "edges.csv")

    # Lecture des fichiers CSV
    nodes_df = pd.read_csv(nodes_csv_path, sep=',')
    edges_df = pd.read_csv(edges_csv_path, sep=',')

    # Connexion Neo4j
    driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", ""))

    # Création des nœuds
    def insert_node(tx, row):
        tx.run("""
            MERGE (e:Entity {id: $id})
            SET e.label = $label, e.name = $name
        """, id=row["id:ID"], label=row["label"], name=row["name"])

    # Création des relations
    def insert_relation(tx, row):
        tx.run("""
            MATCH (a:Entity {id: $start})
            MATCH (b:Entity {id: $end})
            MERGE (a)-[r:REL {type: $type}]->(b)
        """, start=row[":START_ID"], end=row[":END_ID"], type=row["type"])

    with driver.session() as session:
        # Insertion des nœuds
        for _, row in nodes_df.iterrows():
            session.execute_write(insert_node, row)
        # Insertion des relations
        for _, row in edges_df.iterrows():
            session.execute_write(insert_relation, row)

    driver.close()
    print("Import terminé !")



preparer_fichiers_CSV("../data/silver/", "../data/gold/")
inserer_CSV_dans_Neo4j("../data/gold/")

