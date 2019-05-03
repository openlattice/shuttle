package com.openlattice.shuttle.source

import org.slf4j.LoggerFactory
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path

data class LocalFileSource( val path: Path) : IntegrationSource() {

    companion object {
        private val logger = LoggerFactory.getLogger(LocalFileSource::class.java)
    }

    override fun iterator(): Iterator<InputStream> {
        if (!Files.isDirectory(path )){
            return sequenceOf( Files.newInputStream( path ) ).iterator()
        }
        return Files.newDirectoryStream(path) { !Files.isDirectory(it) }
                .map{ Files.newInputStream(it) }.iterator()
    }
}