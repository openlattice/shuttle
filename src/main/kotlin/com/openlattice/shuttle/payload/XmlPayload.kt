package com.openlattice.shuttle.payload

import com.ctc.wstx.stax.WstxInputFactory
import com.ctc.wstx.stax.WstxOutputFactory
import com.fasterxml.jackson.dataformat.xml.XmlFactory
import org.slf4j.LoggerFactory
import java.io.File
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.stream.Stream
import javax.xml.stream.XMLStreamConstants.CHARACTERS
import javax.xml.stream.XMLStreamConstants.END_ELEMENT

data class XmlPayload(val directoryPath: String, val startTagName: String = DEFAULT_START_TAG) : Payload {

    private val logger = LoggerFactory.getLogger(XmlPayload::class.java)

    companion object {
        const val DEFAULT_START_TAG = "Read"
    }

    private val xof = WstxOutputFactory()
    private val xif = WstxInputFactory()
    init {
        xof.configureForSpeed()
        xif.configureForSpeed()
    }
    private val xf = XmlFactory(xif, xof );

    override fun getPayload(): Stream<MutableMap<String, Any>> {
        if (!File( directoryPath ).isDirectory){
            logger.error("XmlPayload only accepts a directory of .xml files. Please specify a directory containing files to be integrated")
            return Stream.of( mutableMapOf<String, Any>() )
        }

        val fileStream = Files.newDirectoryStream(Paths.get( directoryPath ), { filterPath -> filterPath.toString().endsWith(".xml") } )

        return fileStream.map { path ->
            // per file
            Files.newInputStream( path ).use {
                return@map readXmlIntoMap( it, path )
            }
        }.stream()
    }

    private fun readXmlIntoMap( inStream: InputStream, filePath: Path): MutableMap<String, Any> {
        val mutableMapOf = mutableMapOf<String, Any>()

        val xmlReader = xf.xmlInputFactory.createXMLEventReader( inStream )

        var next = xmlReader.nextEvent()
        while ( xmlReader.hasNext() && !(next.isStartElement && next.asStartElement().name.toString() == startTagName) ) {
            next = xmlReader.nextEvent()
            if ( next.isEndDocument){
                logger.error("Start tag $startTagName was not found in the input xml file $filePath");
                return mutableMapOf;
            }
        }
        next = xmlReader.nextEvent()

        var currentContents = ""
        while( !(next.isEndElement && next.asEndElement().name.toString() == startTagName ) ) {
            when ( next.eventType ) {
                END_ELEMENT -> {
                    val endElem = next.asEndElement()
                    mutableMapOf[endElem.name.toString()] = currentContents
                    currentContents = ""
                }
                CHARACTERS -> {
                    val asCharacters = next.asCharacters()
                    currentContents = asCharacters.data
                    if ( asCharacters.isWhiteSpace ) {
                        currentContents = ""
                    }
                }
            }
            next = xmlReader.nextEvent()
        }

        return mutableMapOf
    }

}