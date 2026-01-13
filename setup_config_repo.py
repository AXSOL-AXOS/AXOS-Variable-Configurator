#!/usr/bin/env python3
"""
Script to set up the AXOS_Configs repository.
"""
import os
import shutil
import sys
from pathlib import Path
from typing import Optional, Tuple

from rich.console import Console
from rich.prompt import Prompt, Confirm

# Import the config after it's created
sys.path.append(str(Path(__file__).parent))
from axos_var_configurator.config import DEFAULT_CONFIGS_PATH

console = Console()

def is_git_repo(path: Path) -> bool:
    """Check if the given path is a git repository."""
    try:
        import git
        return git.Repo(path, search_parent_directories=True).git_dir is not None
    except Exception:
        return False

def clean_directory(path: Path) -> bool:
    """Remove all contents of a directory."""
    try:
        for item in path.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
        return True
    except Exception as e:
        console.print(f"[red]Error cleaning directory: {e}")
        return False

def setup_repository(dest: Path) -> Tuple[bool, str]:
    """Set up the git repository at the specified location."""
    import git
    
    if dest.exists():
        if is_git_repo(dest):
            try:
                repo = git.Repo(dest)
                console.print("[blue]Found existing git repository. Pulling latest changes...")
                repo.remotes.origin.pull()
                return True, "Successfully updated repository."
            except Exception as e:
                return False, f"Error pulling changes: {e}"
        else:
            if Confirm.ask(
                f"Directory {dest} exists but is not a git repository. Clean and reinitialize?",
                default=False
            ):
                if not clean_directory(dest):
                    return False, "Failed to clean directory."
            else:
                return False, "Operation cancelled by user."
    
    # Clone the repository
    console.print(f"[blue]Cloning AXOS_Configs repository to {dest}...")
    try:
        git.Repo.clone_from(
            "https://github.com/AXSOL-AXOS/AXOS_Configs.git",
            dest,
            depth=1
        )
        return True, "Successfully cloned repository."
    except Exception as e:
        return False, f"Error cloning repository: {e}"

def main():
    """Main function to set up the configuration repository."""
    console.print("\n[bold]AXOS Config Repository Setup[/bold]\n")
    
    # Get the target directory
    default_path = str(DEFAULT_CONFIGS_PATH)
    custom_path = Prompt.ask(
        f"Enter the path for AXOS_Configs (or press Enter for default: {default_path})",
        default=default_path
    )
    
    target_dir = Path(custom_path.strip('"').strip())
    
    # Set up the repository
    success, message = setup_repository(target_dir)
    
    if success:
        console.print(f"\n[bold green]✓ {message}[/]")
        console.print(f"\n[bold]Setup completed successfully![/]")
        console.print(f"Configuration files are available at: [cyan]{target_dir}/TRLY2501/configs[/]")
    else:
        console.print(f"\n[bold red]✗ {message}[/]")
        
        if "too long" in message.lower():
            console.print("\n[bold]Windows Path Length Limitation Detected[/]")
            console.print("To fix this, you can either:")
            console.print("1. Clone the repository to a shorter path (e.g., C:\\AXSOL_Configs)")
            console.print("2. Enable long path support in Windows")
            console.print("   - Open PowerShell as Administrator")
            console.print("   - Run: [cyan]Set-ItemProperty -Path 'HKLM:\\SYSTEM\\CurrentControlSet\\Control\\FileSystem' -Name 'LongPathsEnabled' -Value 1[/]")
            console.print("   - Restart your computer")
        
        console.print("\n[bold]Troubleshooting Tips:[/]")
        console.print("- Make sure you have Git installed and in your PATH")
        console.print("- Check your internet connection")
        console.print("- Try running as administrator")
        console.print("\nYou can also manually clone the repository:")
        console.print("  git clone https://github.com/AXSOL-AXOS/AXOS_Configs.git")
        console.print(f"  Then update the path in [cyan]axos_var_configurator/config.py[/]")
        
        sys.exit(1)

if __name__ == "__main__":
    main()
