from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Optional


def _run(args: list[str], cwd: Path) -> None:
    subprocess.run(args, cwd=str(cwd), check=True)


def is_git_repo(path: Path) -> bool:
    return (path / ".git").exists()


def git_add(paths: list[str | Path], repo_root: str | Path) -> None:
    """Add files to the git index.
    
    Args:
        paths: List of file paths (as strings or Path objects) to add
        repo_root: Root directory of the git repository
    """
    repo_root = Path(repo_root).resolve()
    
    # Process each path individually to handle any errors
    for path in paths:
        try:
            path = Path(path)
            # Convert to relative path if it's absolute
            if path.is_absolute():
                rel_path = path.relative_to(repo_root)
            else:
                rel_path = path
                
            # Convert to string with forward slashes
            git_path = str(rel_path).replace('\\', '/')
            
            # Add the file
            _run(["git", "add", "--", git_path], cwd=repo_root)
            
        except Exception as e:
            # If one file fails, log the error but continue with others
            print(f"Warning: Could not add {path}: {str(e)}")
            continue


def git_commit(message: str, repo_root: Path) -> None:
    """Create a commit with the given message.
    
    Args:
        message: Commit message
        repo_root: Root directory of the git repository
        
    Raises:
        ValueError: If there are no changes to commit
    """
    # Check if there are any staged changes
    result = subprocess.run(
        ["git", "diff", "--cached", "--quiet"],
        cwd=str(repo_root),
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:  # No staged changes
        raise ValueError("No changes staged for commit. Please add files before committing.")
    
    # If we get here, there are staged changes to commit
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
