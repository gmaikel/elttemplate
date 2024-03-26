import os
import subprocess


def docker_build():
    try:
        # Exécute la commande docker build
        subprocess.run(["docker", "build", ".", "--tag", "customising_airflow:latest"], check=True)
        print("La construction de l'image Docker a été effectuée avec succès.")
    except subprocess.CalledProcessError as e:
        print(f"Une erreur s'est produite lors de la construction de l'image Docker : {e}")


def create_env_file():
    # Obtenez l'ID utilisateur en utilisant la bibliothèque os
    airflow_uid = str(os.getuid())

    # Écrivez l'ID utilisateur dans le fichier .env
    with open('.env', 'w') as env_file:
        env_file.write(f'AIRFLOW_UID={airflow_uid}\n')


if __name__=='__main__':
    # Appel de la fonction pour créer le fichier .env
    create_env_file()
    docker_build()

