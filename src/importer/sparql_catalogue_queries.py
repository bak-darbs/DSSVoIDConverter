CATALOGUE_CLASS_SELECT_QUERY = """
PREFIX void: <http://rdfs.org/ns/void#>
PREFIX sd:   <http://www.w3.org/ns/sparql-service-description#>

SELECT ?service ?classPartition ?class ?entities WHERE {
  VALUES ?service { <__SERVICE_IRI__> }
  ?service a sd:Service ;
           void:classPartition ?classPartition .
  ?classPartition void:class ?class .
  OPTIONAL { ?classPartition void:entities ?entities }
}
ORDER BY ?class ?classPartition ?entities
"""


CATALOGUE_PROPERTY_SELECT_QUERY = """
PREFIX void: <http://rdfs.org/ns/void#>
PREFIX sd:   <http://www.w3.org/ns/sparql-service-description#>

SELECT ?service ?propertyPartition ?property ?triples WHERE {
  VALUES ?service { <__SERVICE_IRI__> }
  ?service a sd:Service ;
           void:propertyPartition ?propertyPartition .
  ?propertyPartition void:property ?property .
  OPTIONAL { ?propertyPartition void:triples ?triples }
}
ORDER BY ?property ?propertyPartition ?triples
"""


CATALOGUE_LINKSET_SELECT_QUERY = """
PREFIX void: <http://rdfs.org/ns/void#>
PREFIX sd:   <http://www.w3.org/ns/sparql-service-description#>
PREFIX catalogue: <https://catalogue.kgmt.org/>

SELECT ?linkset ?sourceClass ?property ?targetClass ?triples WHERE {
  VALUES ?service { <__SERVICE_IRI__> }
  ?service a sd:Service .

  ?linkset a void:Linkset ;
           catalogue:sourceService ?service ;
           void:linkPredicate ?property ;
           void:subjectsTarget ?sourcePartition ;
           void:objectsTarget ?targetPartition .

  ?sourcePartition void:class ?sourceClass .
  ?targetPartition void:class ?targetClass .

  FILTER EXISTS {
    ?service void:propertyPartition ?servicePropertyPartition .
    ?servicePropertyPartition void:property ?property .
  }
  FILTER EXISTS {
    ?service void:classPartition ?serviceSourcePartition .
    ?serviceSourcePartition void:class ?sourceClass .
  }
  FILTER EXISTS {
    ?service void:classPartition ?serviceTargetPartition .
    ?serviceTargetPartition void:class ?targetClass .
  }

  OPTIONAL { ?linkset void:triples ?triples }
}
ORDER BY ?sourceClass ?property ?targetClass ?linkset
"""


CATALOGUE_SERVICE_METADATA_SELECT_QUERY = """
PREFIX sd:      <http://www.w3.org/ns/sparql-service-description#>
PREFIX catalogue: <https://catalogue.kgmt.org/>

SELECT ?service ?endpoint WHERE {
  VALUES ?service { <__SERVICE_IRI__> }
  ?service a sd:Service .
  OPTIONAL { ?service sd:endpoint ?sdEndpoint }
  OPTIONAL { ?service catalogue:endpoint ?catalogueEndpoint }
  OPTIONAL { ?service catalogue:sparqlEndpoint ?catalogueSparqlEndpoint }
  BIND(COALESCE(?sdEndpoint, ?catalogueEndpoint, ?catalogueSparqlEndpoint) AS ?endpoint)
}
LIMIT 1
"""


CATALOGUE_CLASS_ANNOTATIONS_SELECT_QUERY = """
PREFIX void:      <http://rdfs.org/ns/void#>
PREFIX sd:        <http://www.w3.org/ns/sparql-service-description#>
PREFIX catalogue: <https://catalogue.kgmt.org/>
PREFIX rdfs:      <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?class ?annotProp ?annotValue ?language WHERE {
  VALUES ?service { <__SERVICE_IRI__> }
  VALUES ?annotProp { rdfs:label rdfs:comment }

  ?service a sd:Service ;
           void:classPartition ?cp ;
           catalogue:hasLabelRecord ?record .
  ?cp void:class ?class .

  ?record catalogue:labelKind catalogue:ClassLabelKind ;
          catalogue:labelTarget ?class ;
          ?annotProp ?annotValue .

  BIND(LANG(?annotValue) AS ?language)
}
ORDER BY ?class ?annotProp ?language ?annotValue
"""


CATALOGUE_PROPERTY_ANNOTATIONS_SELECT_QUERY = """
PREFIX void:      <http://rdfs.org/ns/void#>
PREFIX sd:        <http://www.w3.org/ns/sparql-service-description#>
PREFIX catalogue: <https://catalogue.kgmt.org/>
PREFIX rdfs:      <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?property ?annotProp ?annotValue ?language WHERE {
  VALUES ?service { <__SERVICE_IRI__> }
  VALUES ?annotProp { rdfs:label rdfs:comment }

  ?service a sd:Service ;
           void:propertyPartition ?pp ;
           catalogue:hasLabelRecord ?record .
  ?pp void:property ?property .

  ?record catalogue:labelKind catalogue:PropertyLabelKind ;
          catalogue:labelTarget ?property ;
          ?annotProp ?annotValue .

  BIND(LANG(?annotValue) AS ?language)
}
ORDER BY ?property ?annotProp ?language ?annotValue
"""


def for_service(query: str, service_iri: str) -> str:
    """Transforms a query with a __SERVICE_IRI__ placeholder into a query for the given service."""
    return query.replace("__SERVICE_IRI__", service_iri)
