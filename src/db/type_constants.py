class CP_REL_TYPE:
    """Class-Property relationship types (cp_rel_types table)."""

    INCOMING = 1
    OUTGOING = 2 
    TYPE_CONSTRAINT = 3
    VALUE_TYPE_CONSTRAINT = 4


class CC_REL_TYPE:
    """Class-Class relationship types (cc_rel_types table)."""

    SUB_CLASS_OF = 1
    EQUIVALENT_CLASS = 2
    INTERSECTING_CLASS = 3


class PP_REL_TYPE:
    """Property-Property relationship types (pp_rel_types table)."""

    FOLLOWED_BY = 1
    COMMON_SUBJECT = 2
    COMMON_OBJECT = 3
    SUB_PROPERTY_OF = 4
