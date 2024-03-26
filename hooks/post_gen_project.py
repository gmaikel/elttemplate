import os
import subprocess


def docker_build() -> None:
    try:
        subprocess.run(["docker", "build", ".", "--tag", "{{cookiecutter.project_slug}}:latest"], check=True)
        print("The Docker image construction was successfully completed.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred during Docker image construction: {e}")


def create_env_file() -> None:
    airflow_uid = str(os.getuid())

    with open('.env', 'w') as env_file:
        env_file.write(f'AIRFLOW_UID={airflow_uid}\n')


if __name__=='__main__':
    create_env_file()
    docker_build()

