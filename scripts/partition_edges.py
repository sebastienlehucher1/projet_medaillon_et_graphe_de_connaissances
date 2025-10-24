import argparse
import math
import os
import pandas as pd


def decouper_edges(rep_silver_path, NUM_SHARDS):

    """
    Découpe les edges en **8 shards** (edge-cut).
    """ 
      
    cleaned_edges_parquet_path = os.path.join(rep_silver_path, "edges.parquet")

    edges_file_name  = cleaned_edges_parquet_path.split("/")[-1]

    try:
        df_edges = pd.read_parquet(cleaned_edges_parquet_path)
        print(f"\nFichier '{edges_file_name}' chargé : {len(df_edges)} lignes.")
    except FileNotFoundError:
        print(f"Erreur : Le fichier '{edges_file_name}' n'a pas été trouvé.")

    # Calcul la taille de chaque shard    
    rows_per_shard = math.ceil(len(df_edges) / NUM_SHARDS)

    for i in range(NUM_SHARDS):
        start = i * rows_per_shard
        end = (i + 1) * rows_per_shard
        shard_df = df_edges.iloc[start:end]

        shard_path = f"{rep_silver_path}shard={i}/"

        # Crée le dossier s'il n'existe pas
        os.makedirs(shard_path, exist_ok=True)

        edges_shard_path = os.path.join(shard_path, "edges.parquet")
        shard_df.to_parquet(edges_shard_path, index=False)
        print(f"Shard {i} sauvegardé : {edges_shard_path}")



parser = argparse.ArgumentParser(description="Script pour traiter des données.")
    
parser.add_argument(
    "--in_dir", 
    type=str,
    required=True,
    help="Chemin du répertoire source des données synthétiques nettoyées."
)

parser.add_argument(
    "--partitions", 
    type=int,
    required=True,
    help="Nombre de partitions."
)

args = parser.parse_args()

rep_silver_path = args.in_dir
nbr_shards = args.partitions

decouper_edges(rep_silver_path, nbr_shards)


