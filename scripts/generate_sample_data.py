import argparse
import os
import numpy as np
import pandas as pd


def generate_nodes_data(rep_raw_path, nbr_lines):    

    """
    Génère un fichier CSV à partir de données aléatoires.
    """
    
    labels = ['Person', 'Org', 'Paper']    
    
    data = {'id': [i for i in range(nbr_lines)]}

    # Crée le DataFrame avec la colonne 'id'
    df = pd.DataFrame(data)

    df['label'] = np.random.choice(labels, size=nbr_lines)

    df['name'] = df['label'].astype(str) + "_" + df['id'].astype(str) 
    df['name'] = df['name'].str.lower()  
    
    # Crée le dossier s'il n'existe pas
    os.makedirs(rep_raw_path, exist_ok=True)

    nodes_file_path = os.path.join(rep_raw_path, "nodes.csv")

    nodes_file_name  = nodes_file_path.split("/")[-1]

    df.to_csv(nodes_file_path, index=False)

    print(f"Les données ont été exportées vers '{nodes_file_name}' avec succès.")


def generate_edges_data(rep_raw_path, nbr_lines): 

    """
    Génère un fichier CSV de relations à partir des IDs de nodes.
    """ 
       
    nodes_file_path = os.path.join(rep_raw_path, "nodes.csv")
    
    df = pd.read_csv(nodes_file_path, sep=",")

    id_list = df['id'].tolist()

    df_edges = pd.DataFrame()

    df_edges['src'] = np.random.choice(id_list, size=nbr_lines)

    df_edges['dst'] = np.random.choice(id_list, size=nbr_lines)

    df_edges['type'] = 'REL'    

    edges_file_path = os.path.join(rep_raw_path, "edges.csv")

    edges_file_name  = edges_file_path.split("/")[-1]

    df_edges.to_csv(edges_file_path, index=False)

    print(f"Les données ont été exportées vers '{edges_file_name}' avec succès.")



parser = argparse.ArgumentParser(description="Script pour traiter des données.")
    
parser.add_argument(
    "--out_dir", 
    type=str,
    required=True,
    help="Chemin du répertoire source des données synthétiques."
)

parser.add_argument(
    "--nodes", 
    type=int,
    required=True,
    help="Nombre de noeuds."
)

parser.add_argument(
    "--edges", 
    type=int,
    required=True,
    help="Nombre de relations."
)

args = parser.parse_args()

rep_raw_path = args.out_dir
nbr_nodes = args.nodes
nbr_edges = args.edges

generate_nodes_data(rep_raw_path, nbr_nodes)
generate_edges_data(rep_raw_path, nbr_edges)