import argparse
import os
import pandas as pd


def convert_CSV_to_Parquet(rep_raw_path, rep_bronze_path):

    """
    Convertit un fichier CSV en un fichier Parquet en utilisant pandas.
    """
    
    nodes_file_path = os.path.join(rep_raw_path, "nodes.csv")

    nodes_file_name  = nodes_file_path.split("/")[-1]

    edges_file_path = os.path.join(rep_raw_path, "edges.csv")

    edges_file_name  = edges_file_path.split("/")[-1]    

    # Crée le dossier s'il n'existe pas
    os.makedirs(rep_bronze_path, exist_ok=True)
    

    nodes_parquet_path = os.path.join(rep_bronze_path, "nodes.parquet")

    nodes_parquet_file_name  = nodes_parquet_path.split("/")[-1]
    

    edges_parquet_path = os.path.join(rep_bronze_path, "edges.parquet")

    edges_parquet_file_name  = edges_parquet_path.split("/")[-1]


    try:
        df_nodes = pd.read_csv(nodes_file_path, sep=',')

        # Écrit le DataFrame au format Parquet (index=False pour éviter d'enregistrer l'index du DataFrame)
        df_nodes.to_parquet(nodes_parquet_path, engine='pyarrow', index=False)

        print(f"Fichier '{nodes_file_name}' lu avec succès. {len(df_nodes)} lignes trouvées.")
        print(f"\nConversion réussie vers '{nodes_parquet_file_name}'!")

    except FileNotFoundError:
        print(f"Erreur : Le fichier '{nodes_file_name}' n'a pas été trouvé.")
    except Exception as e:
        print(f"\nUne erreur s'est produite lors de la conversion : {e}")
    

    try:
        df_edges = pd.read_csv(edges_file_path, sep=',')

        # Écrit le DataFrame au format Parquet (index=False pour éviter d'enregistrer l'index du DataFrame)
        df_edges.to_parquet(edges_parquet_path, engine='pyarrow', index=False)

        print(f"\nFichier '{edges_file_name}' lu avec succès. {len(df_edges)} lignes trouvées.")
        print(f"\nConversion réussie vers '{edges_parquet_file_name}'!")

    except FileNotFoundError:
        print(f"Erreur : Le fichier '{edges_file_name}' n'a pas été trouvé.")
    except Exception as e:
        print(f"\nUne erreur s'est produite lors de la conversion : {e}")



parser = argparse.ArgumentParser(description="Script pour traiter des données.")
    
parser.add_argument(
    "--in_dir", 
    type=str,
    required=True,
    help="Chemin du répertoire source des données synthétiques."
)
    
parser.add_argument(
    "--out_dir", 
    type=str,
    required=True,
    help="Chemin du répertoire des données synthétiques compressées."
)

args = parser.parse_args()

rep_raw_path = args.in_dir
rep_bronze_path = args.out_dir

convert_CSV_to_Parquet(rep_raw_path, rep_bronze_path)




    



