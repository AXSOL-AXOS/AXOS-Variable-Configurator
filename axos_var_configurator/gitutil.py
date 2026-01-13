from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Optional


def _run(args: list[str], cwd: Path) -> None:
    subprocess.run(args, cwd=str(cwd), check=True)


def is_git_repo(path: Path) -> bool:
    return (path / ".git").exists()


def git_add(paths: list[Path], repo_root: Path) -> None:
    rel = [str(p.relative_to(repo_root)) for p in paths]
    _run(["git", "add", "--"] + rel, cwd=repo_root)


def git_commit(message: str, repo_root: Path) -> None:
    _run(["git", "commit", "-m", message], cwd=repo_root)


def git_tag(tag: str, repo_root: Path, message: Optional[str] = None) -> None:
    if message:
        _run(["git", "tag", "-a", tag, "-m", message], cwd=repo_root)
    else:
        _run(["git", "tag", tag], cwd=repo_root)


def git_push(repo_root: Path, remote: str = "origin", branch: str = "main") -> None:
    """Push changes to the specified remote and branch.
    
    Args:
        repo_root: Root directory of the git repository
        remote: Name of the remote to push to (default: 'origin')
        branch: Name of the branch to push (default: 'main')
    """
    _run(["git", "push", remote, branch], cwd=repo_root)
