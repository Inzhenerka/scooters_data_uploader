from typing import Annotated
import typer
from rich import print
from src.github_repo import GitHubRepo
from src.data_manager import DataManager
from src.data_ingestor import DataIngestor

repo: str = 'Inzhenerka/scooters_data_generator'
repo_branch: str = 'main'
tables: list[str] = ['trips', 'users', 'events']

db_uri_arg: Annotated[str, typer.Argument] = typer.Argument(
    default='postgresql://postgres:postgres@localhost:5432/postgres',
    help='Database URI starting with postgresql://',
)
schema_opt: Annotated[str, typer.Option] = typer.Option(
    default='scooters_raw',
    help='Name of schema in database',
)

github_repo = GitHubRepo(
    repo=repo,
    branch=repo_branch
)
data_manager = DataManager(github_repo=github_repo)
data_ingestor = DataIngestor()

app = typer.Typer(
    name='Scooters Data Uploader',
    help='Upload data to database from remote repository and compare versions',
)


def pprint(msg: str, color: str):
    print(f'[{color}]{msg}[/{color}]')


@app.command()
def version(db_uri=db_uri_arg, schema=schema_opt):
    """Compare version of data in database with the latest version available remotely."""
    data_ingestor.create_engine(db_uri=db_uri, schema_name=schema)
    db_version: str | None = data_ingestor.get_version()
    github_version: str = github_repo.get_repo_version()
    print(f"Latest data version available: {github_version}")
    print(f"Current data version in database: {db_version}")
    if not db_version:
        print(":x:  [red]Database is empty. Run 'upload' command to upload data[/red]")
    elif db_version != github_version:
        print(":rotating_light:  [yellow]Database is outdated. Run 'upload' command to upload data[/yellow]")
    else:
        print(":white_check_mark:  [green]Database is up to date, nothing to do[/green]")


@app.command()
def upload(db_uri=db_uri_arg, schema=schema_opt):
    """Upload data to database from remote repository."""
    # Create database engine
    data_ingestor.create_engine(db_uri=db_uri, schema_name=schema)
    # Create schema in database
    pprint(f":gear:  Creating schema [bold]{schema}[/bold] for data...", 'blue')
    data_ingestor.create_schema()
    # Download latest version of data from remote
    pprint(f":cloud:  Getting the latest version of data from remote...", 'green')
    github_version: str = github_repo.get_repo_version()
    # Upload data to database
    for table_name in tables:
        table_file_name: str = f'{table_name}.parquet'
        # Загрузка файла с удаленного источника
        pprint(f'  :arrow_down:  Downloading [bold]{table_file_name}[/bold] from remote...', 'cyan')
        df = data_manager.load_parquet_from_github(table_file_name, version=github_version)
        pprint(
            f'  :arrow_up:  Uploading [bold]{table_file_name}[/bold] to [bold]{table_name}[/bold] table...',
            'green'
        )
        data_ingestor.create_table_from_df(df=df, table_name=table_name)
    # Create data version table
    pprint(
        f':white_check_mark:  Upload complete. Updating stored version to [bold]{github_version}[/bold]...',
        'green'
    )
    data_ingestor.create_version_table(data_version=github_version)


if __name__ == "__main__":
    app()
