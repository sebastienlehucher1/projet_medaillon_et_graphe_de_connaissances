import argparse
import os
import pandas as pd


def verifier_unicite_IDs(rep_bronze_path, rep_silver_path):

    """
    Vérifie l'unicité des IDs dans nodes.
    """

    nodes_parquet_path = os.path.join(rep_bronze_path, "nodes.parquet")

    nodes_file_name  = nodes_parquet_path.split("/")[-1]

    try:
        df_nodes = pd.read_parquet(nodes_parquet_path)
        print(f"Fichier '{nodes_file_name}' chargé : {len(df_nodes)} lignes.")
    except FileNotFoundError:
        print(f"Erreur : Le fichier '{nodes_file_name}' n'a pas été trouvé.")
    

    total_rows = len(df_nodes)
    unique_ids_count = df_nodes['id'].nunique()

    if total_rows == unique_ids_count:
        print(f"L'unicité est parfaite. {total_rows} IDs uniques pour {total_rows} lignes.")
    else:
        duplicates_count = total_rows - unique_ids_count
        print(f"Des doublons ont été trouvés :")
        print(f"  - Total des lignes : {total_rows}")
        print(f"  - IDs uniques : {unique_ids_count}")
        print(f"  - Nombre de doublons (lignes en excès) : {duplicates_count}")
        
    # Conserve uniquement la première occurrence de chaque id
    df_clean = df_nodes.drop_duplicates(subset=['id'], keep='first')

    print("\nDataFrame après suppression des doublons :")
    print(df_clean)    

    cleaned_nodes_parquet_path = os.path.join(rep_silver_path, "nodes.parquet")

    # Écrit le DataFrame au format Parquet (index=False pour éviter d'enregistrer l'index du DataFrame)
    df_clean.to_parquet(cleaned_nodes_parquet_path, engine='pyarrow', index=False)


def verifier_absence_valeurs_nulles_edges(rep_bronze_path, rep_silver_path):

    """
    Vérifie l'absence de valeurs nulles dans src et dst des edges.
    """
  
    edges_parquet_path = os.path.join(rep_bronze_path, "edges.parquet")

    edges_file_name  = edges_parquet_path.split("/")[-1]

    try:
        df_edges = pd.read_parquet(edges_parquet_path)
        print(f"\nFichier '{edges_file_name}' chargé : {len(df_edges)} lignes.")
    except FileNotFoundError:
        print(f"Erreur : Le fichier '{edges_file_name}' n'a pas été trouvé.")
    
    couple_fields = ['src', 'dst']
    
    has_nulls = df_edges[couple_fields].isnull().any().any()

    if not has_nulls:
        print(f"Le fichier '{edges_file_name}' ne contient pas de valeur nulle dans au moins un des deux champs 'src' et 'dst'.")
    else:
        print(f"Le fichier '{edges_file_name}' contient des valeurs nulles dans au moins un des deux champs 'src' et 'dst'.")
    
    # Conserve uniquement les valeurs non nulles dans les champs 'src' et 'dst'
    df_clean = df_edges.dropna(subset=couple_fields, how='any')

    print("\nDataFrame après suppression des valeurs nulles dans les champs 'src' et 'dst':")
    print(df_clean)    

    cleaned_edges_parquet_path = os.path.join(rep_silver_path, "edges.parquet")

    # Écrit le DataFrame au format Parquet (index=False pour éviter d'enregistrer l'index du DataFrame)
    df_clean.to_parquet(cleaned_edges_parquet_path, engine='pyarrow', index=False)


def verifier_unicite_relations_edges(rep_bronze_path, rep_silver_path):

    """
    Vérifie l'unicité des couples de valeur de src et dst dans edges.
    """
   
    edges_parquet_path = os.path.join(rep_bronze_path, "edges.parquet")

    edges_file_name  = edges_parquet_path.split("/")[-1]

    try:
        df_edges = pd.read_parquet(edges_parquet_path)
        print(f"\nFichier '{edges_file_name}' chargé : {len(df_edges)} lignes.")
    except FileNotFoundError:
        print(f"Erreur : Le fichier '{edges_file_name}' n'a pas été trouvé.")
    
    couple_fields = ['src', 'dst']

    # Repère TOUTES les lignes dont la combinaison est dupliquée au moins une fois.
    all_duplicates_mask = df_edges.duplicated(subset=couple_fields, keep=False)

    # Filtre pour obtenir l'ensemble complet des lignes dupliquées
    all_duplicates = df_edges[all_duplicates_mask]

    print("\nToutes les lignes impliquées dans la duplication :")
    print(all_duplicates[['src', 'dst']])

    # Conserve uniquement la première occurrence de chaque couple (src, dst)
    df_unique = df_edges.drop_duplicates(subset=couple_fields, keep='first')

    print("\nDataFrame après suppression des doublons :")
    print(df_unique[['src', 'dst']])    

    cleaned_edges_parquet_path = os.path.join(rep_silver_path, "edges.parquet")

    # Écrit le DataFrame au format Parquet (index=False pour éviter d'enregistrer l'index du DataFrame)
    df_unique.to_parquet(cleaned_edges_parquet_path, engine='pyarrow', index=False)



parser = argparse.ArgumentParser(description="Script pour traiter des données.")
    
parser.add_argument(
    "--in_dir", 
    type=str,
    required=True,
    help="Chemin du répertoire des données synthétiques compressées."
)
    
parser.add_argument(
    "--out_dir", 
    type=str,
    required=True,
    help="Chemin du répertoire des données synthétiques nettoyées."
)

args = parser.parse_args()

rep_bronze_path = args.in_dir
rep_silver_path = args.out_dir

verifier_unicite_IDs(rep_bronze_path, rep_silver_path)
verifier_absence_valeurs_nulles_edges(rep_bronze_path, rep_silver_path)
verifier_unicite_relations_edges(rep_bronze_path, rep_silver_path)