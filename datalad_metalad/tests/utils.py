from pathlib import Path
from uuid import UUID

from datalad.support.gitrepo import GitRepo


def create_dataset(directory: str, id: UUID) -> GitRepo:

    git_repo = GitRepo(directory)

    git_repo_path = Path(git_repo.path)
    datalad_dir = git_repo_path / ".datalad"
    datalad_dir.mkdir()

    datalad_config = datalad_dir / "config"
    datalad_config.write_text(
        '[datalad "dataset"]\n'
        f'\tid = {id}')

    return git_repo


