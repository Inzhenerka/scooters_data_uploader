import requests
import tomllib


class GitHubRepo:
    _repo: str
    _branch: str

    def __init__(self, repo: str, branch: str = 'main'):
        self._repo = repo
        self._branch = branch

    def get_repo_version(self) -> str:
        url: str = f'https://raw.githubusercontent.com/{self._repo}/{self._branch}/pyproject.toml'
        response = requests.get(url)
        # Check if the request was successful
        if response.status_code == 200:
            # Parse the TOML content
            pyproject_toml = tomllib.loads(response.text)
            # Access the version field
            version: str = pyproject_toml['project']['version']
            return version
        else:
            raise RuntimeError(f'Failed to get version from {url}')

    def download_file(self, version: str, file_name: str, local_file_path: str) -> str:
        url: str = f'https://github.com/{self._repo}/releases/download/v{version}/{file_name}'
        response = requests.get(url)
        if response.status_code == 200:
            with open(local_file_path, 'wb') as file:
                file.write(response.content)
            return local_file_path
        else:
            raise RuntimeError(f'Failed to download {url}')
