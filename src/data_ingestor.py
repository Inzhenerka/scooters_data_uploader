import duckdb
from rich import print


class DataIngestor:
    _s3_data_path: str
    _schema_name: str
    _database_name: str = 'postgresql_db'
    _version_table_name: str = 'version'
    _version_file_name: str = 'version.txt'
    _conn: duckdb.DuckDBPyConnection | None = None

    def __init__(
            self,
            s3_data_path: str,
            schema_name: str,
    ):
        self._s3_data_path = s3_data_path.rstrip('/') + '/'
        self._schema_name = schema_name

    def connect(self, postgresql_uri: str) -> None:
        if self._conn:
            self._conn.close()
        self._conn = duckdb.connect()
        postgresql_uri = postgresql_uri.replace('postgres://', 'postgresql://')
        self._conn.execute(f"ATTACH '{postgresql_uri}' AS {self._database_name} (TYPE POSTGRES);")

    def load_data_to_database(self, table_name: str):
        s3_path: str = f'{self._s3_data_path}{table_name}.parquet'
        full_table_name: str = self._full_database_table_name(table_name)
        print(
            f'  :arrow_right:  Writing [bold]{s3_path}[/bold] data to [bold]{full_table_name}[/bold] table...',
        )
        # Recreating database table with data
        self._conn.execute(f"""
            DROP TABLE IF EXISTS {full_table_name} CASCADE;
            CREATE TABLE {full_table_name} AS FROM '{s3_path}';
        """)

    def create_version_table(self, data_version: str):
        print(f'  :arrow_right:  Updating stored date version to [bold]{data_version}[/bold]...')
        full_table_name: str = self._full_database_table_name(self._version_table_name)
        self._conn.execute(f"""
            DROP TABLE IF EXISTS {full_table_name};
            CREATE TABLE {full_table_name} AS 
            SELECT 
                '{data_version}' as data_version,
                now() as updated_at;
        """)

    def get_version(self):
        full_table_name: str = self._full_database_table_name(self._version_table_name)
        query: str = f"""
            SELECT data_version
            FROM {full_table_name}
        """
        return self._conn.execute(query).fetchone()[0]

    def get_remote_version(self) -> str:
        print(f':cloud:  Getting the latest version of data from remote file storage...')
        s3_path: str = self._s3_data_path + self._version_file_name
        return self._conn.execute(f"SELECT content FROM read_text('{s3_path}')").fetchone()[0].strip()

    def create_schema(self):
        print(f':gear:  Creating schema [bold]{self._schema_name}[/bold] for data...')
        self._conn.execute(f'CREATE SCHEMA IF NOT EXISTS {self._database_name}.{self._schema_name};')

    def _full_database_table_name(self, table_name: str) -> str:
        return f'"{self._database_name}"."{self._schema_name}"."{table_name}"'

    def __del__(self):
        if self._conn:
            self._conn.close()
