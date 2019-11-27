package com.openlattice.shuttle

import org.apache.olingo.commons.api.edm.FullQualifiedName

private const val flight = "flight"
enum class FlightFqnConstants(val fqn: FullQualifiedName) {
    DEFINITION(FullQualifiedName(flight, "definition")),
    ARGS(FullQualifiedName(flight, "arguments")),
    CONTACT(FullQualifiedName(flight, "contact")),
    SQL(FullQualifiedName(flight, "sql")),
    PKEY(FullQualifiedName(flight, "primaryKeyColumns"))
}