SERVICE_METADATA_SELECT_QUERY = """
PREFIX sd:      <http://www.w3.org/ns/sparql-service-description#>

SELECT ?service ?endpoint WHERE {
  ?service a sd:Service .
  OPTIONAL { ?service sd:endpoint ?endpoint }
}
ORDER BY ?service
LIMIT 1
"""


CLASS_SELECT_QUERY = """
PREFIX void: <http://rdfs.org/ns/void#>
PREFIX sd:   <http://www.w3.org/ns/sparql-service-description#>

SELECT ?graphDesc ?classPartition ?class ?entities WHERE {
  ?graphDesc a sd:Graph ;
             void:classPartition ?classPartition .
  ?classPartition void:class ?class .
  OPTIONAL { ?classPartition void:entities ?entities }
}
"""


PROPERTY_SELECT_QUERY = """
PREFIX void: <http://rdfs.org/ns/void#>
PREFIX sd:   <http://www.w3.org/ns/sparql-service-description#>

SELECT ?graphDesc ?pp ?prop ?triples WHERE {
  ?graphDesc a sd:Graph ;
             void:propertyPartition ?pp .
  ?pp void:property ?prop .
  OPTIONAL { ?pp void:triples ?triples }
}
"""


PARTITION_CP_RELS_SELECT_QUERY = """
PREFIX void: <http://rdfs.org/ns/void#>
PREFIX sd:   <http://www.w3.org/ns/sparql-service-description#>

SELECT ?sourceClass ?property ?cpTriples ?targetClass ?targetTriples WHERE {
  ?graphDesc a sd:Graph ;
             void:classPartition ?cp .
  ?cp void:class ?sourceClass ;
      void:propertyPartition ?pp .
  ?pp void:property ?property .
  OPTIONAL { ?pp void:triples ?cpTriples }
  OPTIONAL {
    ?pp void:classPartition ?tcp .
    ?tcp void:class ?targetClass .
    OPTIONAL { ?tcp void:triples ?targetTriples }
  }
}
"""


LINKSETS_SELECT_QUERY = """
PREFIX void: <http://rdfs.org/ns/void#>
PREFIX sd:   <http://www.w3.org/ns/sparql-service-description#>

SELECT ?sourceClass ?property ?ot ?targetClass ?triples WHERE {
  ?graphDesc a sd:Graph ;
             void:subset ?linkset .
  ?linkset a void:Linkset ;
           void:linkPredicate ?property .
  OPTIONAL { ?linkset void:subjectsTarget ?st . ?st void:class ?sourceClass }
  OPTIONAL {
    ?linkset void:objectsTarget ?ot .
    OPTIONAL { ?ot void:class ?targetClass }
  }
  OPTIONAL { ?linkset void:triples ?triples }
}
"""


PD_RELS_DATATYPE_QUERY = """
PREFIX void:     <http://rdfs.org/ns/void#>
PREFIX void_ext: <http://ldf.fi/void-ext#>
PREFIX sd:       <http://www.w3.org/ns/sparql-service-description#>

SELECT ?prop ?datatype ?ppTriples WHERE {
  ?graphDesc a sd:Graph ;
             void:propertyPartition ?pp .
  ?pp void:property ?prop ;
      void_ext:datatypePartition ?dp .
  ?dp void_ext:datatype ?datatype .
  OPTIONAL { ?pp void:triples ?ppTriples }
}
"""


CPD_RELS_DATATYPE_QUERY = """
PREFIX void:     <http://rdfs.org/ns/void#>
PREFIX void_ext: <http://ldf.fi/void-ext#>
PREFIX sd:       <http://www.w3.org/ns/sparql-service-description#>
PREFIX xsd:      <http://www.w3.org/2001/XMLSchema#>

SELECT ?class ?prop ?datatype ?ppTriples ?hasObjectTarget ?objectTargetTriplesSum ?hasMissingObjectTargetCount WHERE {
  ?graphDesc a sd:Graph ;
             void:classPartition ?cp .
  ?cp void:class ?class ;
      void:propertyPartition ?pp .
  ?pp void:property ?prop ;
      void_ext:datatypePartition ?dp .
  ?dp void_ext:datatype ?datatype .
  OPTIONAL { ?pp void:triples ?ppTriples }
  OPTIONAL {
    SELECT ?pp
           (SUM(xsd:integer(?targetTriples)) AS ?objectTargetTriplesSum)
           (COUNT(?targetCp) AS ?objectTargetCount)
           (COUNT(?targetTriples) AS ?objectTargetCountWithTriples)
    WHERE {
      ?pp void:classPartition ?targetCp .
      OPTIONAL { ?targetCp void:triples ?targetTriples }
    }
    GROUP BY ?pp
  }
  BIND(BOUND(?objectTargetCount) && ?objectTargetCount > 0 AS ?hasObjectTarget)
  BIND(BOUND(?objectTargetCount) && ?objectTargetCountWithTriples < ?objectTargetCount AS ?hasMissingObjectTargetCount)
}
"""


def build_unresolved_ot_fallback_query(vocab_partition_uri: str) -> str:
    return (
        "PREFIX void: <http://rdfs.org/ns/void#>\n"
        "SELECT ?fallbackClass WHERE {\n"
        f"  <{vocab_partition_uri}> void:class ?fallbackClass .\n"
        "}"
    )
