package com.openlattice.shuttle.payload

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.dataformat.xml.XmlMapper
import org.slf4j.LoggerFactory
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.util.stream.Stream

data class XmlPayload( val directoryPath: String ) : Payload {

    private val logger = LoggerFactory.getLogger(XmlPayload::class.java)

    private val mapper = XmlMapper()

    override fun getPayload(): Stream<MutableMap<String, Any>> {
        if (!File( directoryPath ).isDirectory){
            logger.error("XmlPayload only accepts a directory of .xml files. Please specify a directory containing files to be integrated")
            return Stream.of( mutableMapOf<String, Any>() )
        }

        val fileStream = Files.newDirectoryStream(Paths.get( directoryPath ), { filterPath -> filterPath.toString().endsWith(".xml") } )

        return fileStream.map { path ->
            // per file
            Files.newInputStream( path ).use {
                return@map mapper.readValue(it, object : TypeReference<MutableMap<String, Any>>(){}) as MutableMap<String, Any>
            }
        }.stream()
    }
}