package com.openlattice.shuttle.payload

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.dataformat.xml.XmlMapper
import com.openlattice.shuttle.source.IntegrationOrigin
import com.openlattice.shuttle.source.LocalFileOrigin
import org.slf4j.LoggerFactory
import java.nio.file.Paths
import java.util.stream.Stream
import kotlin.streams.asStream

data class XmlFilesPayload(val origin: IntegrationOrigin) : Payload {

    constructor( source: String ) : this( LocalFileOrigin( Paths.get( source ), { it.toString().endsWith(xmlSuffix) } ) )

    companion object {
        private val logger = LoggerFactory.getLogger(XmlFilesPayload::class.java)
        private val xmlSuffix = ".xml"
        private val mapper = XmlMapper()
    }

    override fun getPayload(): Stream<MutableMap<String, Any>> {
        return origin.map {
            mapper.readValue(it, object : TypeReference<MutableMap<String, Any>>(){}) as MutableMap<String, Any>
        }.asStream()
    }
}