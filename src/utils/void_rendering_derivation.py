from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

from src.db.type_constants import CP_REL_TYPE


def derive_cp_rel_object_cnt(
    type_id: int,
    cnt: Optional[int],
    property_iri: str,
    has_cpc_rels: bool,
    has_cpd_rels: bool,
    classification_property_iri: str = "",
) -> Optional[int]:
    """
    Derive cp_rels.object_cnt from already imported direct-VoID evidence.

    - any row with class-pair evidence is object-bearing
    - datatype-only OUTGOING rows resolve to 0
    - configured classification-property rows are object-bearing
    """
    if property_iri == classification_property_iri:
        return cnt

    if type_id == CP_REL_TYPE.OUTGOING:
        if has_cpc_rels:
            return cnt
        if has_cpd_rels:
            return 0
        return None

    if type_id == CP_REL_TYPE.INCOMING:
        if has_cpc_rels:
            return cnt
        return None

    return None


def derive_property_object_cnt(
    property_cnt: Optional[int],
    cp_object_cnts: Iterable[Optional[int]],
) -> Optional[int]:
    """Derive properties.object_cnt from related cp_rels rows."""
    values = list(cp_object_cnts)
    if not values:
        return None

    known_values = [value for value in values if value is not None]
    if any(value > 0 for value in known_values):
        return property_cnt

    if (
        known_values
        and len(known_values) == len(values)
        and all(value == 0 for value in known_values)
    ):
        return 0

    return None


def _set_coverage(subset: Iterable[Any], superset: Iterable[Any]) -> float:
    subset_values = set(subset)
    if not subset_values:
        return 0.0
    superset_values = set(superset)
    return len(subset_values & superset_values) / len(subset_values)


def _is_cover_set_candidate_covered(
    candidate: Dict[str, Any],
    selected: Dict[str, Any],
) -> bool:
    """
    Return whether an already selected row makes this candidate redundant.

    This is an approximate substitute for cover-set logic. A candidate is
    considered covered only when all of these are true:

    - its count is not meaningfully larger than the selected row's count;
    - most of its objects are also present on the selected row's class;
    - when it has class targets, most of those targets are also present
      on the selected row.
    """
    candidate_cnt = candidate.get("cnt")
    selected_cnt = selected.get("cnt")
    if candidate_cnt is None or selected_cnt is None:
        return False
    if candidate_cnt > (selected_cnt * 1.05):
        return False

    signature_coverage = _set_coverage(
        candidate.get("signature", ()),
        selected.get("signature", ()),
    )
    if signature_coverage < 0.75:
        return False

    pair_signature = candidate.get("pair_signature", ())
    selected_pair_signature = selected.get("pair_signature", ())
    if pair_signature:
        pair_coverage = _set_coverage(pair_signature, selected_pair_signature)
        if pair_coverage < 0.75:
            return False

    return True


def _cover_set_sort_key(candidate: Dict[str, Any]) -> Tuple[float, float, str]:
    cnt = candidate.get("cnt")
    class_cnt = candidate.get("class_cnt")
    return (
        -cnt if cnt is not None else float("inf"),
        class_cnt if class_cnt is not None else float("inf"),
        candidate.get("iri") or "",
    )


def rank_cover_set(
    candidates: Iterable[Dict[str, Any]],
) -> Dict[int, int]:
    """
    Choose which rows should be treated as principal for visual rendering.

    Each candidate is a cp_rels or cpc_rels row for the same property.
    Rows with stronger counts are considered first. A row gets the
    next positive cover_set_index when it adds a meaningfully different
    class profile from the rows already selected. It stays 0 when an
    already selected row appears to cover the same property/class-pair pattern.

    The result maps candidate["id"] to the assigned cover_set_index.
    """
    ordered = sorted(candidates, key=_cover_set_sort_key)
    if not ordered:
        return {}

    ranked = {candidate["id"]: 0 for candidate in ordered}
    selected: List[Dict[str, Any]] = []
    for candidate in ordered:
        if any(
            _is_cover_set_candidate_covered(candidate, winner) for winner in selected
        ):
            continue

        selected.append(candidate)
        ranked[candidate["id"]] = len(selected)

    if not selected:
        ranked[ordered[0]["id"]] = 1
    return ranked


def backfill_cp_rel_object_cnt(
    current_object_cnt: Optional[int],
    type_id: int,
    cnt: Optional[int],
    property_has_object_rows: bool,
    has_cpd_rels: bool,
) -> Optional[int]:
    """
    Fill unresolved cp_rels.object_cnt 
    """
    if current_object_cnt is not None:
        return current_object_cnt
    if cnt is None:
        return None

    if type_id == CP_REL_TYPE.INCOMING:
        if property_has_object_rows:
            return cnt
        return None

    if type_id == CP_REL_TYPE.OUTGOING:
        if property_has_object_rows:
            if not has_cpd_rels:
                return cnt
            return None
        return 0

    return None
