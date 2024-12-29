from typing import Annotated
import typer
from rich import print
from src.data_ingestor import DataIngestor

s3_data_path: str = 's3://inzhenerka-public/scooters_data_generator'
tables: list[str] = ['trips', 'users', 'events']
default_database_uri: str = 'postgresql://postgres:postgres@localhost:5432/postgres'
default_schema_name: str = 'scooters_raw'

DatabaseURI = Annotated[str, typer.Argument(
    help='Database URI starting with postgresql://',
)]
SchemaName = Annotated[str, typer.Option(
    help='Name of schema in database',
)]

app = typer.Typer(
    name='Scooters Data Uploader',
    help='Upload data to database from remote repository and compare versions',
)


@app.command()
def version(
        database_uri: DatabaseURI = default_database_uri,
        schema_name: SchemaName = default_schema_name
):
    """Compare version of data in database with the latest version available remotely."""
    data_ingestor = DataIngestor(s3_data_path=s3_data_path, schema_name=schema_name)
    data_ingestor.connect(database_uri)
    remote_data_version = data_ingestor.get_remote_version()
    current_data_version = data_ingestor.get_version()
    printc(f"Latest data version available: {remote_data_version}")
    printc(f"Current data version in database: {current_data_version}")
    if not current_data_version:
        printc(":x:  Database is empty. Run 'upload' command to upload data", 'red')
    elif current_data_version != remote_data_version:
        printc(":rotating_light:  Database is outdated. Run 'upload' command to upload data", 'yellow')
    else:
        printc(":white_check_mark:  Database is up to date, nothing to do", 'green')


@app.command()
def upload(
        database_uri: DatabaseURI = default_database_uri,
        schema_name: SchemaName = default_schema_name
):
    """Upload data to database from remote repository."""
    data_ingestor = DataIngestor(s3_data_path=s3_data_path, schema_name=schema_name)
    data_ingestor.connect(database_uri)
    data_version: str = data_ingestor.get_remote_version()
    data_ingestor.create_schema()
    for table_name in tables:
        data_ingestor.load_data_to_database(table_name=table_name)
    data_ingestor.create_version_table(data_version=data_version)
    printc(
        f':white_check_mark:  Data successfully uploaded to schema [bold]{schema_name}[/bold]', 'green'
    )


def printc(msg: str, color: str = 'white'):
    print(f'[{color}]{msg}[/{color}]')


if __name__ == "__main__":
    app()
