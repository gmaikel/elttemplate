import subprocess
import os
from dotenv import dotenv_values


def create_venv(env_name, python_version):
    subprocess.run(['python', '-m', 'venv', env_name])
    print(f"L'environnement venv '{env_name}' avec Python {python_version} a été créé avec succès.")


def create_conda_env(env_name, python_version):
    subprocess.run(['conda', 'create', '--name', env_name, '--yes', f'python={python_version}'])
    print(f"L'environnement conda '{env_name}' avec Python {python_version} a été créé avec succès.")


def activate_env(env_name, environment_manager):
    if environment_manager == 'conda':
        subprocess.run(['conda', 'config', '--set', 'auto_activate_base', 'false'])
        activate_cmd = subprocess.Popen(['conda', 'activate', env_name], shell=True, stdout=subprocess.PIPE)
        activate_cmd.communicate()
    elif environment_manager == 'venv':
        activate_cmd = subprocess.Popen([os.path.join(env_name, 'Scripts' if os.name == 'nt' else 'bin', 'activate')],
                                         shell=True, stdout=subprocess.PIPE)
        activate_cmd.communicate()
    print(f"L'environnement '{env_name}' a été activé.")


def create_and_install_env(env_file='.env', requirements_file='requirements.txt'):
    try:
        # Charger les données depuis le fichier .env
        env_data = dotenv_values(env_file)

        # Extraire les valeurs nécessaires
        env_name = env_data.get('ENV_NAME', 'test-env')
        python_version = env_data.get('PYTHON_VERSION', '3.10')
        environment_manager = env_data.get('ENVIRONMENT_MANAGER', 'conda')

        # Création de l'environnement
        if environment_manager == 'conda':
            create_conda_env(env_name, python_version)
        elif environment_manager == 'venv':
            create_venv(env_name, python_version)

        # Activation de l'environnement
        activate_env(env_name, environment_manager)

        # Installation des dépendances
        if environment_manager == 'conda':
            subprocess.check_call(['conda', 'run', '-n', env_name, 'pip', 'install', '-r', requirements_file])
        else:
            pip_path = os.path.join(env_name, 'Scripts' if os.name == 'nt' else 'bin', 'pip')
            subprocess.run([pip_path, 'install', '-r', requirements_file])
        print("Les dépendances ont été installées avec succès !")

        # Changement de répertoire vers "plugins"
        plugins_dir = os.path.join(os.getcwd(), 'plugins')
        os.chdir(plugins_dir)
        print(f"Changement de répertoire vers {plugins_dir}")

        # Initialisation de dbt en mode modèle
        subprocess.run(['dbt', 'init', '-s', 'transform'])
        print("dbt a été instancié avec succès en mode modèle dans le dossier plugins.")

    except FileNotFoundError:
        print("Il semble que conda, pip ou le fichier .env ne soit pas trouvé sur votre système.")
    except Exception as e:
        print(f"Une erreur inattendue s'est produite : {e}")


# Appel de la fonction pour créer et installer l'environnement avec les dépendances et instancier dbt en mode modèle dans le dossier plugins
create_and_install_env()
