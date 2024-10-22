import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


class DataIngestor:
    _engine: Engine
    _schema_name: str

    def create_engine(self, db_uri: str, schema_name: str):
        self._engine = create_engine(db_uri)
        self._schema_name = schema_name

    def create_schema(self):
        self._execute_sql(f'CREATE SCHEMA IF NOT EXISTS {self._schema_name}')

    def create_table_from_df(self, table_name: str, df: pd.DataFrame):
        # Drop the existing table if it exists
        self._execute_sql(
            f'DROP TABLE IF EXISTS "{self._schema_name}"."{table_name}" CASCADE'
        )
        # Upload the DataFrame to PostgreSQL
        df.to_sql(
            name=table_name,
            con=self._engine,
            schema=self._schema_name,
            index=False,
            if_exists='replace',
            method='multi',
            chunksize=1000,
        )

    def create_version_table(self, data_version: str):
        df = pd.DataFrame([{
            'data_version': data_version,
            'updated_at': pd.Timestamp.now(tz='UTC')
        }])
        self.create_table_from_df('version', df)

    def get_version(self) -> str | None:
        query: str = f'SELECT data_version FROM "{self._schema_name}"."version"'
        df = pd.read_sql(query, self._engine)
        if df.shape[0] > 0:
            return df.iloc[0].iloc[0]

    def _execute_sql(self, sql: str):
        with self._engine.begin() as conn:
            conn.execute(text(sql))
