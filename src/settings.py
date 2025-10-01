from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    PyprojectTomlConfigSettingsSource,
    SettingsConfigDict,
)


class Settings(BaseSettings):
    @classmethod
    def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (PyprojectTomlConfigSettingsSource(settings_cls),)


class UploaderSettings(Settings):
    s3_data_path: str
    tables: list[str]
    version_table_name: str
    version_file_name: str
    default_database_uri: str
    default_schema_name: str
    bot_url: str
    raw_data_url: str

    model_config = SettingsConfigDict(
        pyproject_toml_table_header=('tool', 'uploader')
    )
