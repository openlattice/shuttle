package com.openlattice.shuttle.payload

import com.openlattice.shuttle.util.CsvUtil
import com.openlattice.shuttle.util.CsvUtil.newDefaultMapper
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException

private val logger = LoggerFactory.getLogger(CsvPayload::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class CsvPayload(val path: String) : Payload {
    override fun getPayload(): Iterable<Map<String, Any?>> {
        return try {
            return object : Iterable<Map<String, Any?>> {
                override fun iterator(): Iterator<Map<String, Any?>> {
                    return newDefaultMapper()
                            .readerFor(Map::class.java)
                            .with(CsvUtil.newDefaultSchemaFromHeader())
                            .readValues<Map<String, Any?>>(File(path))
                }
            }
        } catch (e: IOException) {
            logger.error("Unable to read csv file", e)
            return listOf()
        }
    }
}