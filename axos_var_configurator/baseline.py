from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional


@dataclass(frozen=True)
class BaselinePlan:
    name: str
    db_path: Path
    baseline_dir: Path
    include_files: list[Path]
    exclude_names: list[str]


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _iter_db_csvs(db_path: Path) -> Iterable[Path]:
    if not db_path.exists() or not db_path.is_dir():
        return []
    return sorted(p for p in db_path.iterdir() if p.is_file() and p.suffix.lower() == ".csv")


def plan_baseline(
    name: str,
    db_path: Path,
    repo_root: Path,
    exclude_names: Optional[list[str]] = None,
) -> BaselinePlan:
    exclude_names = exclude_names or []
    include = [p for p in _iter_db_csvs(db_path) if p.name not in set(exclude_names)]
    baseline_dir = (repo_root / "baselines" / name).resolve()
    return BaselinePlan(
        name=name,
        db_path=db_path.resolve(),
        baseline_dir=baseline_dir,
        include_files=include,
        exclude_names=exclude_names,
    )


def apply_baseline(plan: BaselinePlan) -> Path:
    out_db = plan.baseline_dir / "database"
    out_db.mkdir(parents=True, exist_ok=True)

    manifest_files: list[dict[str, object]] = []

    for src in plan.include_files:
        dst = out_db / src.name
        shutil.copy2(src, dst)
        manifest_files.append(
            {
                "file": f"database/{src.name}",
                "sha256": _sha256(dst),
                "size": dst.stat().st_size,
            }
        )

    manifest = {
        "name": plan.name,
        "createdAt": datetime.now(timezone.utc).isoformat(),
        "sourceDb": str(plan.db_path),
        "excluded": list(plan.exclude_names),
        "files": manifest_files,
    }

    manifest_path = plan.baseline_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    return plan.baseline_dir
