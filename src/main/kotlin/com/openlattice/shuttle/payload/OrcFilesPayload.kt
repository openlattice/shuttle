package com.openlattice.shuttle.payload

import com.google.common.collect.Lists
import com.google.common.collect.Maps
import com.openlattice.shuttle.source.LocalFileOrigin
import org.apache.hadoop.conf.Configuration
import org.apache.orc.OrcFile
import org.apache.orc.RecordReader
import org.apache.orc.storage.ql.exec.vector.*
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*

class OrcFilesPayload( val origin: LocalFileOrigin ) : Payload {

    constructor(source: String) : this(LocalFileOrigin(Paths.get(source)) {
        it.toString().endsWith(OrcFilesPayload.ORC_SUFFIX)
    })

    companion object {
        private val logger = LoggerFactory.getLogger(OrcFilesPayload::class.java)
        private const val ORC_SUFFIX = ".orc"
    }

    override fun getPayload(): Iterable<Map<String, Any?>> {
        return OrcIterable( origin.path )
    }
}

class OrcIterable(val path: Path): Iterable<Map<String, Any?>> {
    override fun iterator(): Iterator<Map<String, Any?>> {
        return OrcIterator( path )
    }
}

class OrcIterator(val path: Path): Iterator<Map<String, Any?>> {

    private val orcConfiguration = Configuration()
    private var currentBatch: VectorizedRowBatch
    private var rows: RecordReader
    private var currentBuffer: ArrayList<Map<String, Any>> = Lists.newArrayList()
    private var currentBufferIndex: Int = 0
    private var fields: ArrayList<String>

    init {
        orcConfiguration.setBoolean("header", true)
        orcConfiguration.setBoolean("inferSchema", true)
    }

    val orcReader = OrcFile.createReader(
            org.apache.hadoop.fs.Path(path.toUri()),
            OrcFile.ReaderOptions(orcConfiguration)
    )

    init {
        rows = orcReader.rows()
        currentBatch= orcReader.schema.createRowBatch()
        val fieldNames = orcReader.schema.fieldNames
        fields = ArrayList<String>( fieldNames.size )
        fieldNames.forEachIndexed { index, s ->  fields.add(index, s) }
    }

    override fun hasNext(): Boolean {
        return !currentBatch.endOfFile
    }

    fun mapRow( rowIndex: Int ): Map<String, Any> {
        val map = Maps.newLinkedHashMapWithExpectedSize<String, Any>(currentBatch.numCols)
        currentBatch.cols.forEachIndexed { index: Int, columnVector: ColumnVector? ->
            when (columnVector?.type) {
                ColumnVector.Type.LONG -> {
                    map.put(fields[index], (columnVector as LongColumnVector).vector[rowIndex])
                }
                ColumnVector.Type.DOUBLE -> {
                    map.put(fields[index], (columnVector as DoubleColumnVector).vector[rowIndex])
                }
                ColumnVector.Type.BYTES -> {
                    map.put(fields[index], (columnVector as BytesColumnVector).vector[rowIndex])
                }
                ColumnVector.Type.DECIMAL -> {
                    map.put(fields[index], (columnVector as DecimalColumnVector).vector[rowIndex])
                }
                ColumnVector.Type.DECIMAL_64 -> {
                    map.put(fields[index], (columnVector as Decimal64ColumnVector).vector[rowIndex])
                }
                ColumnVector.Type.TIMESTAMP,
                ColumnVector.Type.INTERVAL_DAY_TIME,
                ColumnVector.Type.STRUCT,
                ColumnVector.Type.LIST,
                ColumnVector.Type.MAP,
                ColumnVector.Type.UNION,
                ColumnVector.Type.VOID,
                ColumnVector.Type.NONE,
                null -> {
                    null
                }
            }
        }
        return map
    }

    fun mapBatchToRealTypes(): ArrayList<Map<String, Any>> {
        val buffer = Lists.newArrayListWithCapacity<Map<String, Any>>(currentBatch.size)
        for (i in 0 until currentBatch.size ) {
            buffer.add(mapRow(i))
        }
        return buffer
    }

    override fun next(): Map<String, Any?> {
        // end of buffer, refresh
        if ( currentBufferIndex == currentBuffer.size ) {
            currentBufferIndex = 0
            rows.nextBatch( currentBatch )
            currentBuffer = mapBatchToRealTypes()
        }
        return currentBuffer[currentBufferIndex++]
    }
}
