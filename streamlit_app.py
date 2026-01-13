from __future__ import annotations

import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional

import git
import pandas as pd
import streamlit as st
from git import Repo, InvalidGitRepositoryError

from axos_var_configurator.baseline import apply_baseline, plan_baseline
from axos_var_configurator.config import DEFAULT_CONFIGS_PATH, DATABASE_DEVICES_PATH
from axos_var_configurator.csvio import (
    detect_csv_kind,
    load_axsol_abstractions_by_prefix,
    read_csv_rows,
    scan_database
)
from axos_var_configurator.exporter import export_device_json


DEFAULT_DB_PATH = Path(
    r"C:\Users\SimonFeuerbacher\AXSOL GmbH\AXSOL - 05_Entwicklung\20_Software und Steuerung\99_DataTransfer\DataBase_Devices"
)
DEFAULT_BASELINE_EXCLUDES = ["AXSOL_DataBase_Collection_AD_temp_.csv"]
DEFAULT_UI_BACKUP_DIR = (Path.cwd() / "ui_backups").resolve()


def _resolve_db_path(db: Optional[str]) -> Path:
    if db and db.strip():
        return Path(db).expanduser().resolve()
    return DEFAULT_DB_PATH.expanduser().resolve()


def _id_column_for_kind(kind: str) -> str:
    if kind == "device":
        return "Topic"
    if kind == "axsol_abstraction":
        return "AXSOL_Name_Short"
    return ""


def _sanitize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in df.columns:
        df[c] = df[c].astype("string")
        df[c] = df[c].fillna("")
    return df


def _sanitize_folder(name: str) -> str:
    name = name.strip()
    if not name:
        return "_"
    import re

    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name)


def _planned_changes(old_df: pd.DataFrame, new_df: pd.DataFrame, id_col: str) -> dict[str, object]:
    old_df = _sanitize_df(old_df)
    new_df = _sanitize_df(new_df)

    old_ids = set(old_df[id_col].astype(str).str.strip()) if id_col in old_df.columns else set()
    new_ids = set(new_df[id_col].astype(str).str.strip()) if id_col in new_df.columns else set()

    added = sorted([i for i in new_ids if i and i not in old_ids])
    removed = sorted([i for i in old_ids if i and i not in new_ids])

    old_by_id = {str(r[id_col]).strip(): r for r in old_df.to_dict(orient="records") if str(r.get(id_col, "")).strip()}
    new_by_id = {str(r[id_col]).strip(): r for r in new_df.to_dict(orient="records") if str(r.get(id_col, "")).strip()}

    changed: list[dict[str, str]] = []
    for rid in sorted(old_ids.intersection(new_ids)):
        o = old_by_id.get(rid)
        n = new_by_id.get(rid)
        if o is None or n is None:
            continue
        for col in new_df.columns:
            ov = "" if o.get(col) is None else str(o.get(col))
            nv = "" if n.get(col) is None else str(n.get(col))
            if ov != nv:
                changed.append({"row": rid, "field": col, "before": ov, "after": nv})

    return {"added": added, "removed": removed, "changed": changed}


def _write_csv_from_df(csv_path: Path, original_fieldnames: list[str], df: pd.DataFrame) -> None:
    df = _sanitize_df(df)

    out_rows: list[dict[str, str]] = []
    records = df.to_dict(orient="records")
    for r in records:
        out_rows.append({fn: ("" if r.get(fn) is None else str(r.get(fn))) for fn in original_fieldnames})

    from axos_var_configurator.edit import _atomic_write

    _atomic_write(csv_path, fieldnames=original_fieldnames, rows=out_rows)


def _backup_csv(csv_path: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = DEFAULT_UI_BACKUP_DIR / ts
    out_dir.mkdir(parents=True, exist_ok=True)
    dst = out_dir / csv_path.name
    shutil.copy2(csv_path, dst)
    return dst


def main() -> None:
    st.set_page_config(page_title="AXOS Variable Configurator", layout="wide")

    st.title("AXOS Variable Configurator")

    db_input = st.text_input("Database folder", value=str(DEFAULT_DB_PATH))
    db_path = _resolve_db_path(db_input)

    if not db_path.exists():
        st.error("Database folder does not exist")
        return

    tab_edit, tab_create_json, tab_baseline, tab_git = st.tabs(["Edit CSV", "Create JSON", "Baselines", "Git"])

    with tab_edit:
        st.subheader("Edit CSV")
        scan = scan_database(db_path)
        csvs = sorted([*scan.axsol_abstraction_files, *scan.device_files, *scan.unknown_files], key=lambda p: p.name.lower())

        selected = st.selectbox("CSV file", options=csvs, format_func=lambda p: p.name)
        kind = detect_csv_kind(selected)
        id_col = _id_column_for_kind(kind)

        fieldnames, rows = read_csv_rows(selected)
        if not fieldnames:
            st.warning("Selected CSV has no header")
            return

        df = pd.DataFrame(rows, columns=fieldnames)
        df = _sanitize_df(df)

        st.caption(f"Detected kind: {kind}")
        if id_col:
            st.caption(f"Identifier column: {id_col}")
        else:
            st.warning("This CSV kind is not editable (unknown schema).")
            return

        if "_original_df" not in st.session_state or st.session_state.get("_original_path") != str(selected):
            st.session_state["_original_df"] = df.copy()
            st.session_state["_original_path"] = str(selected)

        edited_df = st.data_editor(
            df,
            num_rows="dynamic",
            use_container_width=True,
            key="editor",
        )
        edited_df = _sanitize_df(edited_df)

        st.markdown("### Planned changes")
        plan = _planned_changes(st.session_state["_original_df"], edited_df, id_col=id_col)

        st.write({"added": len(plan["added"]), "removed": len(plan["removed"]), "changed": len(plan["changed"])})

        if plan["changed"]:
            st.dataframe(pd.DataFrame(plan["changed"]), use_container_width=True)
        if plan["added"]:
            st.info(f"Added rows: {plan['added']}")
        if plan["removed"]:
            st.warning(f"Removed rows: {plan['removed']}")

        st.markdown("### Safeguard")
        confirm = st.checkbox("I understand this will write changes to the CSV file.")
        typed = st.text_input("Type APPLY to enable saving", value="")

        can_apply = confirm and typed.strip() == "APPLY"
        if st.button("Apply changes", type="primary", disabled=not can_apply):
            if plan["removed"]:
                st.error("Row deletions are currently blocked in UI for safety. Please undo deletions (or use CLI).")
            else:
                backup_path = _backup_csv(selected)
                _write_csv_from_df(selected, original_fieldnames=fieldnames, df=edited_df)
                st.success(f"Saved. Backup created at: {backup_path}")
                st.session_state["_original_df"] = edited_df.copy()

    with tab_create_json:
        st.subheader("Create JSON Files")
        
        # Scan for device files
        scan = scan_database(db_path)
        device_files = scan.device_files
        
        # File selection
        col1, col2 = st.columns(2)
        with col1:
            export_all = st.checkbox("Export all devices", value=True)
            mode = st.selectbox("Export mode", options=["both", "original", "axsol"], index=0)
        
        if export_all:
            selection = device_files
        else:
            selection = st.multiselect("Select device CSVs to export", 
                                     options=device_files, 
                                     format_func=lambda p: p.name)
        
        # Output options
        st.subheader("Export Options")
        out_dir = st.text_input("Output folder", value=str((Path.cwd() / "out_all").resolve()))
        
        # Git options
        st.subheader("Git Options")
        use_git = st.checkbox("Use Git for version control", value=True)
        
        git_commit_msg = ""
        if use_git:
            git_commit_msg = st.text_area("Commit message", 
                                        value="Update device configurations",
                                        help="Enter a descriptive commit message")
        
        # Action buttons
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Export to Folder", type="primary"):
                if not selection:
                    st.warning("Please select at least one device to export")
                else:
                    with st.spinner("Exporting JSON files..."):
                        try:
                            out_path = Path(out_dir)
                            out_path.mkdir(parents=True, exist_ok=True)
                            
                            abstractions = load_axsol_abstractions_by_prefix(db_path)
                            
                            for device_file in selection:
                                try:
                                    # Create a subdirectory for each device based on the CSV filename
                                    device_name = device_file.stem
                                    device_out_dir = out_path / device_name
                                    
                                    # For 'both' mode, create 'original' and 'axsol' subdirectories
                                    modes = [mode] if mode != "both" else ["original", "axsol"]
                                    
                                    for mode_dir in modes:
                                        mode_out_dir = device_out_dir / mode_dir if mode != "both" else device_out_dir
                                        mode_out_dir.mkdir(parents=True, exist_ok=True)
                                        
                                        export_device_json(
                                            device_file,
                                            abstractions=abstractions,
                                            mode=mode_dir if mode == "both" else mode,
                                            out_dir=mode_out_dir
                                        )
                                    
                                except Exception as e:
                                    st.error(f"Error exporting {device_file.name}: {str(e)}")
                            
                            st.success(f"Successfully exported {len(selection)} device(s) to {out_path}")
                            
                        except Exception as e:
                            st.error(f"Export failed: {str(e)}")
        
        with col2:
            if use_git and st.button("Commit to Git", type="secondary"):
                if not selection:
                    st.warning("Please select at least one device to commit")
                elif not git_commit_msg.strip():
                    st.warning("Please enter a commit message")
                else:
                    try:
                        import git
                        from axos_var_configurator.gitutil import git_add, git_commit, git_push
                        
                        # Get the repository
                        repo_path = DATABASE_DEVICES_PATH.parent  # Assuming DATABASE_DEVICES_PATH is in the repo
                        repo = git.Repo(repo_path)
                        
                        # Only add files that are within the repository
                        files_to_commit = []
                        for device_file in selection:
                            try:
                                # Check if file is in the repository
                                rel_path = device_file.relative_to(repo_path)
                                files_to_commit.append(device_file)
                            except ValueError:
                                st.warning(f"File {device_file} is not in the Git repository and will not be committed")
                                continue
                        
                        if not files_to_commit:
                            st.error("No files to commit - all selected files are outside the repository")
                            return
                            
                        # Add and commit each file
                        for device_file in files_to_commit:
                            # Get relative path from repo root
                            rel_path = device_file.relative_to(repo_path)
                            git_add(repo, str(rel_path))
                        
                        # Commit changes
                        commit_hash = git_commit(repo, git_commit_msg)
                        st.success(f"Successfully committed changes with hash: {commit_hash}")
                        
                        # Option to push
                        if st.button("Push to Remote"):
                            try:
                                git_push(repo)
                                st.success("Successfully pushed changes to remote repository")
                            except Exception as e:
                                st.error(f"Failed to push to remote: {str(e)}")
                        
                    except Exception as e:
                        st.error(f"Git operation failed: {str(e)}")
                        st.exception(e)  # Show full traceback for debugging

    with tab_baseline:
        st.subheader("Baselines")

        name = st.text_input("Baseline name", value="baseline_001")
        repo_root = Path.cwd().resolve()

        plan = plan_baseline(
            name=name,
            db_path=db_path,
            repo_root=repo_root,
            exclude_names=DEFAULT_BASELINE_EXCLUDES,
        )

        st.write(
            {
                "output": str(plan.baseline_dir),
                "files": len(plan.include_files),
                "excluded": plan.exclude_names,
            }
        )

        confirm_baseline = st.checkbox("I understand this will create a baseline snapshot folder in this repo.")
        if st.button("Create baseline", type="primary", disabled=not confirm_baseline):
            out = apply_baseline(plan)
            st.success(f"Created baseline at {out}")

    with tab_git:
        st.subheader("Create and Push to Git")
        st.info("This section helps you commit and push your configuration changes to Git.")
        
        # Show current config directory
        config_dir = DATABASE_DEVICES_PATH.parent
        st.write(f"**Configuration directory:** `{config_dir}`")
        
        # Initialize git repo if not already
        repo = None
        try:
            repo = git.Repo(str(config_dir), search_parent_directories=True)
            repo_root = Path(repo.working_tree_dir).resolve()
            st.success(f"✅ Found Git repository at `{repo_root}`")
            
            try:
                scope_rel = config_dir.resolve().relative_to(repo_root)
                st.write(f"**Working in folder:** `{scope_rel}`")
                
                # Show current branch
                if repo.head.is_detached:
                    st.warning("⚠️ You are in 'detached HEAD' state. Please checkout a branch.")
                else:
                    st.write(f"**Current branch:** `{repo.active_branch.name}`")
                
                # Show remote information
                if not repo.remotes:
                    st.warning("⚠️ No remote repositories configured.")
                else:
                    st.write("**Remotes:**")
                    for remote in repo.remotes:
                        st.write(f"- {remote.name}: {remote.url}")
                
                # Show status
                st.subheader("Current Changes")
                changed_files = [item.a_path for item in repo.index.diff(None)] + repo.untracked_files
                
                if not changed_files:
                    st.success("✅ No uncommitted changes.")
                else:
                    st.warning(f"⚠️ Found {len(changed_files)} uncommitted changes:")
                    for file in changed_files:
                        st.write(f"- {file}")
                    
                    # Commit section
                    with st.form("commit_form"):
                        st.subheader("Commit Changes")
                        commit_message = st.text_area(
                            "Commit message",
                            value="Update configuration",
                            help="Enter a descriptive message about the changes you're committing"
                        )
                        
                        # Stage all changes by default
                        stage_all = st.checkbox("Stage all changes", value=True)
                        
                        if st.form_submit_button("Create Commit"):
                            try:
                                with st.spinner("Creating commit..."):
                                    if stage_all:
                                        repo.git.add(all=True)
                                    
                                    # Create the commit
                                    repo.index.commit(commit_message)
                                    st.success(f"✅ Successfully created commit: {repo.head.commit.hexsha[:7]}")
                                    st.session_state.last_commit = repo.head.commit.hexsha
                                    
                                    # Update the changed files list
                                    changed_files = []
                                    st.experimental_rerun()
                                    
                            except Exception as e:
                                st.error(f"❌ Failed to create commit: {str(e)}")
                
                # Push section (only show if there are remotes)
                if repo.remotes and 'last_commit' in st.session_state:
                    st.subheader("Push to Remote")
                    
                    if st.button("Push Changes", type="primary"):
                        try:
                            with st.spinner("Pushing to remote..."):
                                remote = repo.remotes[0]  # Use the first remote
                                push_info = remote.push()[0]
                                
                                if push_info.flags & push_info.ERROR:
                                    st.error(f"❌ Failed to push: {push_info.summary}")
                                elif push_info.flags & push_info.UP_TO_DATE:
                                    st.info("ℹ️ Everything up-to-date")
                                else:
                                    st.success(f"✅ Successfully pushed to {remote.name}/{repo.active_branch.name}")
                                    
                        except Exception as e:
                            st.error(f"❌ Failed to push: {str(e)}")
                            
            except ValueError:
                st.error(f"The config directory is not inside the detected Git repo. Repo root: `{repo_root}`")
                repo = None
                
        except InvalidGitRepositoryError:
            st.warning("No Git repository found in this folder or any parent folder.")
            
            
            with st.expander("Initialize New Git Repository"):
                st.write("Initialize a new Git repository to start tracking changes.")
                repo_name = st.text_input("Repository name", value="AXOS_Configs")
                
                if st.button("Initialize Repository"):
                    try:
                        # Create the repository one level up from the config directory
                        repo_path = config_dir.parent / repo_name
                        repo_path.mkdir(exist_ok=True)
                        
                        # Initialize the repository
                        repo = git.Repo.init(str(repo_path))
                        
                        # Move the config directory into the new repository
                        new_config_dir = repo_path / config_dir.name
                        if not new_config_dir.exists():
                            config_dir.rename(new_config_dir)
                        
                        # Create a .gitignore file
                        gitignore = repo_path / ".gitignore"
                        gitignore_content = (
                            "# Python\n"
                            "__pycache__/\n"
                            "*.py[cod]\n"
                            "*$py.class\n\n"
                            "# Virtual Environment\n"
                            "venv/\n"
                            "env/\n\n"
                            "# IDE\n"
                            ".idea/\n"
                            ".vscode/\n"
                            "*.swp\n"
                            "*.swo\n\n"
                            "# OS\n"
                            ".DS_Store\n"
                            "Thumbs.db\n\n"
                            "# Project specific\n"
                            "out/\n"
                            "*.csv\n"
                            "*.json"
                        )
                        gitignore.write_text(gitignore_content)
                        
                        # Add and commit initial files
                        repo.git.add(all=True)
                        repo.index.commit("Initial commit")
                        
                        st.success(f"✅ Successfully initialized Git repository at `{repo_path}`")
                        st.info(f"Please refresh the page to continue with the new repository.")
                        
                    except Exception as e:
                        st.error(f"❌ Failed to initialize Git repository: {e}")
        
        except Exception as e:
            st.error(f"❌ Error accessing Git repository: {e}")
            st.exception(e)
            return
            
        if repo is not None:
            # Show repository status
            st.subheader("Repository Status")
            
            try:
                # Get repository information
                if repo.head.is_detached:
                    st.warning("⚠️ You are in 'detached HEAD' state. Please checkout a branch.")
                else:
                    st.write(f"**Current branch:** `{repo.active_branch.name}`")
                
                # Show remote information
                if not repo.remotes:
                    st.warning("⚠️ No remote repositories configured.")
                else:
                    st.write("**Remotes:**")
                    for remote in repo.remotes:
                        st.write(f"- {remote.name}: {remote.url}")
                
                # Show status of files
                st.subheader("File Status")
                
                # Get changed files
                changed = [item.a_path for item in repo.index.diff(None)]
                untracked = repo.untracked_files
                
                if not (changed or untracked):
                    st.success("✅ No uncommitted changes.")
                else:
                    if changed:
                        st.warning("⚠️ Modified files:")
                        for file in changed:
                            st.write(f"- {file}")
                    if untracked:
                        st.warning("⚠️ Untracked files:")
                        for file in untracked:
                            st.write(f"- {file}")
                    
                    # Commit form
                    with st.form("commit_form"):
                        st.subheader("Commit Changes")
                        commit_message = st.text_area(
                            "Commit message",
                            value="Update configuration",
                            help="Enter a descriptive message about the changes you're committing"
                        )
                        
                        if st.form_submit_button("Create Commit"):
                            try:
                                with st.spinner("Creating commit..."):
                                    # Stage all changes
                                    repo.git.add(all=True)
                                    
                                    # Create the commit
                                    repo.index.commit(commit_message)
                                    st.success(f"✅ Successfully created commit: {repo.head.commit.hexsha[:7]}")
                                    st.session_state.last_commit = repo.head.commit.hexsha
                                    st.experimental_rerun()
                                    
                            except Exception as e:
                                st.error(f"❌ Failed to create commit: {str(e)}")
                
                # Push changes if there's a remote
                if repo.remotes and 'last_commit' in st.session_state:
                    st.subheader("Push to Remote")
                    
                    if st.button("Push Changes", type="primary"):
                        try:
                            with st.spinner("Pushing to remote..."):
                                remote = repo.remotes[0]  # Use the first remote
                                push_info = remote.push()[0]
                                
                                if push_info.flags & push_info.ERROR:
                                    st.error(f"❌ Failed to push: {push_info.summary}")
                                elif push_info.flags & push_info.UP_TO_DATE:
                                    st.info("ℹ️ Everything up-to-date")
                                else:
                                    st.success(f"✅ Successfully pushed to {remote.name}/{repo.active_branch.name}")
                                    
                        except Exception as e:
                            st.error(f"❌ Failed to push: {str(e)}")
                            
            except Exception as e:
                st.error(f"❌ Error getting repository status: {e}")
                st.exception(e)
                return
                
                # Show detailed file status
                if changed_files or untracked_files or tracked_unmodified:
                    with st.expander("View File Status", expanded=True):
                        if changed_files:
                            st.write("**Modified Files**")
                            for f in sorted(changed_files):
                                st.code(f"M {f}", language="bash")
                        
                        if untracked_files:
                            st.write("**Untracked Files**")
                            for f in sorted(untracked_files):
                                st.code(f"? {f}", language="bash")
                        
                        if tracked_unmodified:
                            st.write("**Tracked Files**")
                            # Show first 10 files with a "Show more" button if there are many
                            show_all = st.toggle("Show all tracked files", value=False, key="show_all_tracked")
                            files_to_show = tracked_unmodified if show_all else tracked_unmodified[:10]
                            
                            for f in sorted(files_to_show):
                                st.code(f"  {f}", language="bash")
                            
                            if not show_all and len(tracked_unmodified) > 10:
                                st.write(f"... and {len(tracked_unmodified) - 10} more tracked files")
                    
                    # Stage and commit
                    st.subheader("Stage & Commit")
                    commit_message = st.text_area(
                        "Commit message", 
                        value=f"Update configurations - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                        key="commit_message"
                    )
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("Stage All Changes"):
                            try:
                                # Get relative path from repo root to config dir
                                scope_rel = config_dir.resolve().relative_to(repo_path.resolve())
                                # Add all changes in the config directory
                                repo.git.add("*", "--", str(scope_rel))
                                st.success(f"Staged all changes in `{scope_rel}`")
                                # Force a rerun to update the status
                                st.experimental_rerun()
                            except Exception as e:
                                st.error(f"Failed to stage changes: {e}")
                    
                    with col2:
                        if st.button("Commit Changes"):
                            try:
                                # Check if there are any staged changes
                                staged_changes = repo.git.diff("--cached", "--name-status").strip()
                                
                                if not staged_changes:
                                    st.warning("No staged changes to commit. Please stage your changes first.")
                                elif not commit_message.strip():
                                    st.error("Please enter a commit message")
                                else:
                                    # Commit the staged changes
                                    repo.index.commit(commit_message)
                                    st.success(f"Committed changes: {commit_message}")
                                    # Force a rerun to update the status
                                    st.experimental_rerun()
                            except Exception as e:
                                st.error(f"Failed to commit: {e}")
                    
                    # Push to remote
                    st.subheader("Push to Remote")
                    remote_name = st.text_input("Remote name", value="origin")
                    branch_name = st.text_input("Branch name", value="main")
                    
                    if st.button("Push to Remote"):
                        try:
                            with st.spinner("Pushing to remote..."):
                                # Check if remote exists, if not add it
                                if remote_name not in [r.name for r in repo.remotes]:
                                    repo.create_remote(remote_name, "https://github.com/AXSOL-AXOS/AXOS_Configs.git")
                                
                                # Push to remote
                                repo.git.push("-u", remote_name, f"HEAD:{branch_name}")
                                st.success(f"Successfully pushed to {remote_name}/{branch_name}")
                        except Exception as e:
                            st.error(f"Failed to push: {e}")
                            st.exception(e)
                else:
                    st.info("No uncommitted changes in the repository.")
                
                # Branch information
                st.subheader("Branch Information")
                current_branch = repo.active_branch.name
                st.write(f"Current branch: **{current_branch}**")
                
                # Remote tracking
                try:
                    remote = repo.remotes[0]
                    st.write(f"Remote: {remote.name} - {next(remote.urls)}")
                except (IndexError, StopIteration):
                    st.warning("No remote repository configured")
                
            except Exception as e:
                st.error(f"Error getting repository status: {e}")


if __name__ == "__main__":
    main()
