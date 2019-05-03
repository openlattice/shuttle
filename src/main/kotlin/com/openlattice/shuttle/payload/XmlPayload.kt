package com.openlattice.shuttle.payload

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.dataformat.xml.XmlMapper
import com.openlattice.shuttle.source.IntegrationSource
import com.openlattice.shuttle.source.LocalFileSource
import org.slf4j.LoggerFactory
import java.nio.file.Paths
import java.util.stream.Stream
import kotlin.streams.asStream

data class XmlPayload( val source: IntegrationSource) : Payload {
    constructor( source: String ) : this( LocalFileSource( Paths.get( source ) ) )

    companion object {
        private val logger = LoggerFactory.getLogger(XmlPayload::class.java)
        private val mapper = XmlMapper()
    }

    override fun getPayload(): Stream<MutableMap<String, Any>> {
        return source.map {
            mapper.readValue(it, object : TypeReference<MutableMap<String, Any>>(){}) as MutableMap<String, Any>
        }.asStream()
    }
}