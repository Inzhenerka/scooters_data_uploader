from typing import Annotated, List
import typer
from rich import print
import duckdb
from src.settings import UploaderSettings
from src.data_ingestor import DataIngestor

settings = UploaderSettings()

DatabaseURI = Annotated[str, typer.Argument(
    help='Database URI starting with postgresql://',
)]
SchemaName = Annotated[str, typer.Option(
    help='Name of schema in database',
)]

app = typer.Typer(
    name='Scooters Data Uploader by Inzhenerka.Tech',
    help='Upload data to database from remote repository and compare versions',
    no_args_is_help=True,
)


@app.command()
def version(
        database_uri: DatabaseURI = settings.default_database_uri,
        schema_name: SchemaName = settings.default_schema_name
):
    """Compare version of data in database with the latest version available remotely."""
    data_ingestor = DataIngestor(settings=settings, schema_name=schema_name)
    with duckdb.connect() as conn:
        data_ingestor.attach_database(conn, postgresql_uri=database_uri)
        remote_data_version = data_ingestor.get_remote_version(conn)
        current_data_version = data_ingestor.get_version(conn)
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
        database_uri: DatabaseURI = settings.default_database_uri,
        schema_name: SchemaName = settings.default_schema_name
):
    """Upload data to database from remote repository."""
    data_ingestor = DataIngestor(settings=settings, schema_name=schema_name)
    with duckdb.connect() as conn:
        data_ingestor.attach_database(conn, postgresql_uri=database_uri)
        data_version: str = data_ingestor.get_remote_version(conn)
        data_ingestor.create_schema(conn)
        for table_name in settings.tables:
            data_ingestor.load_data_to_database(conn, table_name=table_name)
        data_ingestor.create_version_table(conn, data_version=data_version)
    printc(
        f':white_check_mark:  Data successfully uploaded to schema [bold]{schema_name}[/bold]', 'green'
    )


@app.command()
def bot():
    """Open dbt Data Bot in Telegram."""
    printc("Opening dbt Data Bot in Telegram")
    typer.launch(settings.bot_url)


@app.command()
def sql():
    """Download SQL file with raw data."""
    printc("Opening raw SQL data in browser for download")
    typer.launch(settings.raw_data_url)


def printc(msg: str, color: str = 'white'):
    print(f'[{color}]{msg}[/{color}]')


if __name__ == "__main__":
    app()
