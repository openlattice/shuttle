package com.openlattice.shuttle.payload

import com.ctc.wstx.stax.WstxInputFactory
import com.ctc.wstx.stax.WstxOutputFactory
import com.fasterxml.jackson.dataformat.xml.XmlFactory
import com.ximpleware.VTDGen
import com.ximpleware.VTDNav
import org.slf4j.LoggerFactory
import java.io.File
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.stream.Stream
import javax.xml.stream.XMLStreamConstants.*

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

    private fun readXmlIntoMapVTD( inStream: InputStream, filePath: Path ): MutableMap<String, Any> {
        val mutableMapOf = mutableMapOf<String, Any>()

        val vg = VTDGen();
        val bytes = inStream.readAllBytes()
        vg.setDoc(bytes)
        vg.parse(true)
        val nav = vg.nav
        nav.toElement(VTDNav.NEXT_SIBLING, startTagName)
        if (nav.toString(nav.currentIndex) != startTagName) {
            logger.error("Start tag $startTagName was not found in the input xml file $filePath");
            vg.clear()
            return mutableMapOf
        }
        var previousIndex = nav.currentIndex
        nav.toElement(VTDNav.FIRST_CHILD)
        while ( nav.currentIndex != previousIndex ) {
            val key = nav.toString(nav.currentIndex )
            if ( nav.text != -1 ) {
                mutableMapOf[key] = nav.toNormalizedString( nav.text )
            }
            previousIndex = nav.currentIndex
            nav.toElement(VTDNav.NEXT_SIBLING)
        }
        vg.clear()
        return mutableMapOf
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
                START_ELEMENT -> {
                }
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
                END_DOCUMENT -> {
                    println("[endDoc]")
                }
                else -> {
                    println("[else]")
                }
            }
            next = xmlReader.nextEvent()
        }

        return mutableMapOf
    }

}