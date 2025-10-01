class UploaderConfig:
    s3_data_path: str = "https://inzhenerka-public.s3.eu-west-1.amazonaws.com/scooters_data_generator/"
    tables: list[str] = ["trips", "users", "events"]
    version_table_name: str = "version"
    version_file_name: str = "version.txt"
    default_database_uri: str = "postgresql://postgres:postgres@localhost:5432/postgres"
    default_schema_name: str = "scooters_raw"
    bot_url: str = "https://t.me/inzhenerka_dbt_bot"
    raw_data_url: str = "https://inzhenerka-public.s3.eu-west-1.amazonaws.com/scooters_data_generator/scooters_raw.sql"
