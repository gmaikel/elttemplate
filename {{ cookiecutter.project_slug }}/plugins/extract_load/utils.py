import time
import json
from pathlib import Path
from dataclasses import dataclass

import requests
from snowflake.connector import connect, SnowflakeConnection
from tqdm import tqdm
from airflow.models import Variable


DATA_PATH = Path('/usr/local/airflow/plugins/extract_load/data')
DATABASE_SCHEMA_PATH = DATA_PATH / 'schema.json'


class DirManagement:
    def __init__(
            self,
            nom_schema: str = None,
            nom_table: str = None
    ) -> None:
        self.nom_schema = nom_schema
        self.nom_table = nom_table

        try:
            self._database = json.load(open(DATABASE_SCHEMA_PATH, 'r'))
            self.schema = self._database[self.nom_schema]

            if self.nom_table is not None:
                self.schema = {self.nom_table: self.schema[self.nom_table]}

        except Exception as e:
            raise Exception(f"Error while extracting data:    {e}")

        self.schema = list(self.schema.items())


class ExtractData(DirManagement):
    def __init__(
            self,
            nom_schema: str = None,
            nom_table: str = None
    ) -> None:
        super().__init__(nom_schema, nom_table)

    def extract(self) -> None:
        for table in self.schema:
            table_name = table[0]
            api_info = table[1]
            table_path = DATA_PATH / self.nom_schema / table_name
            table_path.mkdir(parents=True, exist_ok=True)

            response = requests.get(url=api_info['url'],
                                    headers=api_info['headers'],
                                    params=api_info['params'])

            if response.status_code != 200:
                raise Exception(f"Error: {response}")
            else:
                data = response.json()
                timestamp_actuel = int(time.time())
                with open(table_path / f'{timestamp_actuel}.json', 'w') as f:
                    json.dump(data, f)
                print("ok")


class LoadData(DirManagement):
    def __init__(
            self,
            nom_schema: str = None,
            nom_table: str = None
    ) -> None:
        super().__init__(nom_schema, nom_table)
        self._connection = self._snowflake_connection()
        self._cursor = self._connection.cursor()

    def load(self) -> None:
        for table in self.schema:
            table_name = table[0]
            table_path = DATA_PATH / self.nom_schema / table_name
            json_file = self._last_file(table_path=table_path)

            with json_file.open('r') as file:
                extracted_data = json.load(file)

            data = extracted_data['data']

            column_names = list(data[0].keys())
            timestamp_load = int(time.time())
            timestamp_extarct = extracted_data['timestamp']

            sql_statement = (f"INSERT INTO {self.nom_schema}.{table_name} "
                             f"({', '.join(column_names)}, loaded_at, extracted_at) VALUES ({', '.join(['%s'] * len(column_names))}, {timestamp_load}, {timestamp_extarct})")

            tqdm_desc = f"Inserting data into {table_name} table"

            try:
                for row in tqdm(data, desc=tqdm_desc):
                    self._cursor.execute(command=sql_statement,
                                         params=tuple(row.values()))
            except Exception as e:
                print(f"Error in table {table_name} : ", e)
                continue

        self._cursor.close()
        self._connection.close()
        print("ok")

    @staticmethod
    def _last_file(table_path) -> Path:
        files_name = list(table_path.glob('*'))
        return sorted(files_name)[-1]

    @staticmethod
    def _snowflake_connection() -> SnowflakeConnection:
        connection = connect(account=DbtProfilesParameters.account,
                             user=DbtProfilesParameters.user,
                             password=DbtProfilesParameters.password,
                             warehouse=DbtProfilesParameters.warehouse,
                             database=DbtProfilesParameters.database,
                             role=DbtProfilesParameters.role
                             )
        return connection


@dataclass
class DbtProfilesParameters:
    account: str = Variable.get("account")
    user: str = Variable.get('user')
    password: str = Variable.get('password')
    warehouse: str = Variable.get('warehouse')
    database: str = Variable.get('database')
    role: str = Variable.get('role')
