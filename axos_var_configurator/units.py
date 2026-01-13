from __future__ import annotations

from typing import Optional


_UNIT_FACTORS: dict[tuple[str, str], float] = {
    ("w", "kw"): 0.001,
    ("kw", "w"): 1000.0,
    ("va", "kva"): 0.001,
    ("kva", "va"): 1000.0,
    ("var", "kvar"): 0.001,
    ("kvar", "var"): 1000.0,
    ("wh", "kwh"): 0.001,
    ("kwh", "wh"): 1000.0,
    ("kwh", "mwh"): 0.001,
    ("mwh", "kwh"): 1000.0,
    ("wh", "mwh"): 0.000001,
    ("mwh", "wh"): 1000000.0,
}


def _norm(u: Optional[str]) -> Optional[str]:
    if u is None:
        return None
    return str(u).strip().lower()


def convert_unit_factor(from_unit: Optional[str], to_unit: Optional[str]) -> float:
    fu = _norm(from_unit)
    tu = _norm(to_unit)

    if fu is None or tu is None:
        return 1.0
    if fu == tu:
        return 1.0

    return _UNIT_FACTORS.get((fu, tu), 1.0)
