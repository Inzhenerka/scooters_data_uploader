import os
import pandas as pd
from src.github_repo import GitHubRepo


class DataManager:
    _github_repo: GitHubRepo
    _data_dir: str

    def __init__(self, github_repo: GitHubRepo, data_dir: str | None = None):
        self._github_repo = github_repo
        if data_dir:
            self._data_dir = data_dir
        else:
            root_dir: str = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')
            self._data_dir = os.path.abspath(os.path.join(root_dir, 'data'))

    def load_parquet_from_github(self, file_name: str, version: str | None = None) -> pd.DataFrame:
        if not version:
            version = self._github_repo.get_repo_version()
        file_path: str = self.get_file_path(file_name, sub_dir=version)
        if not os.path.exists(file_path):
            self._github_repo.download_file(version, file_name, file_path)
        return self.load_parquet(file_name, sub_dir=version)

    def get_file_path(self, file_name: str, sub_dir: str = '') -> str:
        sub_dir_path: str = os.path.join(self._data_dir, sub_dir)
        if not os.path.exists(sub_dir_path):
            os.makedirs(sub_dir_path)
        return os.path.abspath(os.path.join(sub_dir_path, file_name))

    def load_parquet(self, file_name: str, sub_dir: str = '') -> pd.DataFrame:
        file_path: str = os.path.abspath(os.path.join(self._data_dir, sub_dir, file_name))
        return pd.read_parquet(file_path)
