from duckdb import DuckDBPyConnection
from rich import print
from src.settings import UploaderSettings


class DataIngestor:
    _local_database_name: str = 'postgresql_db'
    _settings: UploaderSettings
    _schema_name: str

    def __init__(
            self,
            settings: UploaderSettings,
            schema_name: str
    ):
        self._settings = settings
        self._schema_name = schema_name

    def attach_database(self, conn: DuckDBPyConnection, postgresql_uri: str) -> None:
        postgresql_uri = postgresql_uri.replace('postgres://', 'postgresql://')
        conn.execute(f"ATTACH '{postgresql_uri}' AS {self._local_database_name} (TYPE POSTGRES);")

    def load_data_to_database(self, conn: DuckDBPyConnection, table_name: str):
        s3_path: str = f'{self._settings.s3_data_path}{table_name}.parquet'
        full_table_name: str = self._full_database_table_name(table_name)
        print(
            f'  :arrow_right:  Writing [bold]{s3_path}[/bold] data to [bold]{full_table_name}[/bold] table...',
        )
        # Recreating database table with data
        conn.execute(f"""
            DROP TABLE IF EXISTS {full_table_name} CASCADE;
            CREATE TABLE {full_table_name} AS FROM '{s3_path}';
        """)

    def create_version_table(self, conn: DuckDBPyConnection, data_version: str):
        print(f'  :arrow_right:  Updating stored date version to [bold]{data_version}[/bold]...')
        full_table_name: str = self._full_database_table_name(self._settings.version_table_name)
        conn.execute(f"""
            DROP TABLE IF EXISTS {full_table_name};
            CREATE TABLE {full_table_name} AS 
            SELECT 
                '{data_version}' as data_version,
                now() as updated_at;
        """)

    def get_version(self, conn: DuckDBPyConnection):
        full_table_name = self._full_database_table_name(self._settings.version_table_name)
        query = f"""
            SELECT data_version
            FROM {full_table_name}
        """
        return conn.execute(query).fetchone()[0]

    def get_remote_version(self, conn: DuckDBPyConnection) -> str:
        print(f':cloud:  Getting the latest version of data from remote file storage...')
        s3_path: str = self._settings.s3_data_path + self._settings.version_file_name
        results = conn.execute(f"SELECT content FROM read_text('{s3_path}')").fetchone()
        if not results:
            raise RuntimeError(f'Unable to get remote version from {s3_path}')
        return str(results[0].strip())

    def create_schema(self, conn: DuckDBPyConnection) -> None:
        print(f':gear:  Creating schema [bold]{self._schema_name}[/bold] for data...')
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS {self._local_database_name}.{self._schema_name};')

    def _full_database_table_name(self, table_name: str) -> str:
        return f'"{self._local_database_name}"."{self._schema_name}"."{table_name}"'
