from __future__ import annotations

from typing import Dict, Iterable, Optional, Tuple


def select_incoming_principal_source(
    sources: Iterable[Tuple[str, Optional[int]]],
    class_iri_to_cnt: Dict[str, Optional[int]],
) -> Optional[Tuple[str, int, Optional[int]]]:
    """
    Choose the incoming principal source approximation from pair data.

    Ranking mirrors the validated DSS-style approximation:
    highest pair count, then smallest source class count, then lexical IRI.
    """
    candidates = []
    for source_iri, pair_cnt in sources:
        if pair_cnt is None:
            continue
        candidates.append((source_iri, pair_cnt, class_iri_to_cnt.get(source_iri)))

    if not candidates:
        return None

    candidates.sort(
        key=lambda item: (
            -item[1],
            item[2] if item[2] is not None else float("inf"),
            item[0],
        )
    )
    return candidates[0]


def resolve_incoming_cp_rel_count(
    sources: Iterable[Tuple[str, Optional[int]]],
    class_iri_to_id: Dict[str, int],
    class_iri_to_cnt: Dict[str, Optional[int]],
    sum_pair_cnt: Optional[int],
) -> Tuple[Optional[int], Optional[int], str]:
    """
    Resolve incoming principal_class_id and cp_rels.cnt.

    Preferred mode is the validated principal-pair approximation. If no usable
    principal source can be selected, fall back to the accumulated pair count
    for the target class and property.
    """
    principal = select_incoming_principal_source(sources, class_iri_to_cnt)
    if principal is not None:
        principal_source_iri, principal_pair_cnt, _source_class_cnt = principal
        principal_class_id = class_iri_to_id.get(principal_source_iri)
        if principal_class_id is not None:
            return principal_class_id, principal_pair_cnt, "PRINCIPAL_PAIR"

    if sum_pair_cnt is not None:
        return None, sum_pair_cnt, "FALLBACK_SUM_PAIR"
    return None, None, "FALLBACK_NULL"


def resolve_pd_rel_count_from_property_total(
    property_cnt: Optional[int],
    datatype_iris: Iterable[str],
    has_object_branch: bool,
) -> Tuple[Optional[int], str]:
    unique_datatypes = {iri for iri in datatype_iris if iri}
    if len(unique_datatypes) != 1:
        return None, "AMBIGUOUS_MULTIPLE_DATATYPES"
    if has_object_branch:
        return None, "AMBIGUOUS_OBJECT_BRANCH"
    if property_cnt is None:
        return None, "MISSING_PROPERTY_TOTAL"
    return property_cnt, "PROPERTY_TOTAL"


def resolve_pd_rel_count_from_sources(
    cpd_count_sum: Optional[int],
    property_cnt: Optional[int],
    datatype_iris: Iterable[str],
    has_object_branch: bool,
) -> Tuple[Optional[int], str]:
    """
    Resolve pd_rels.cnt from approved datatype evidence.

    The preferred fallback is the sum of matching cpd_rels.cnt values,
    even though source classes can overlap. The user explicitly approved that
    overlap-tolerant rule. If no summed CPD count is available, fall back to
    the older conservative single-datatype/no-object property-total rule.
    """
    if cpd_count_sum is not None:
        return cpd_count_sum, "SUM_CPD_RELS"
    return resolve_pd_rel_count_from_property_total(
        property_cnt,
        datatype_iris,
        has_object_branch,
    )


def resolve_cpd_rel_count_from_partition_remainder(
    parent_partition_cnt: Optional[int],
    datatype_iris: Iterable[str],
    has_object_branch: bool,
    object_target_count_sum: Optional[int],
    has_missing_object_target_count: bool,
) -> Tuple[Optional[int], str]:
    """
    Resolve cpd_rels.cnt from nested VoID partition evidence.

    For datatype-only branches, the parent property-partition count is safe to
    assign directly. For mixed object/datatype branches, infer a datatype count
    only when all object target counts are known and exactly one datatype is
    present, so the remainder can be assigned unambiguously.
    """
    unique_datatypes = {iri for iri in datatype_iris if iri}
    if parent_partition_cnt is None:
        return None, "MISSING_PARENT_PARTITION_TOTAL"

    if not has_object_branch:
        return parent_partition_cnt, "PARENT_PARTITION_TOTAL"

    if len(unique_datatypes) != 1:
        return None, "AMBIGUOUS_MULTIPLE_DATATYPES"
    if has_missing_object_target_count:
        return None, "MISSING_OBJECT_TARGET_COUNT"
    if object_target_count_sum is None:
        return None, "MISSING_OBJECT_TARGET_COUNT"

    remainder = parent_partition_cnt - object_target_count_sum
    if remainder < 0:
        return None, "NEGATIVE_REMAINDER"
    return remainder, "PARTITION_REMAINDER"
