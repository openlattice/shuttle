package com.openlattice.shuttle.payload

import com.google.common.collect.ImmutableList
import com.google.common.collect.Lists
import com.google.common.collect.Maps
import com.openlattice.shuttle.source.LocalFileOrigin
import org.apache.hadoop.conf.Configuration
import org.apache.orc.OrcFile
import org.apache.orc.RecordReader
import org.apache.orc.storage.ql.exec.vector.*
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.nio.file.Paths

class OrcFilesPayload(
        val origin: LocalFileOrigin,
        val spark: SparkSession?
) : Payload {

    constructor(source: String, spark: SparkSession? ) : this(LocalFileOrigin(Paths.get(source)) {
        it.toString().endsWith(OrcFilesPayload.ORC_SUFFIX)
    }, spark)

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
    private var currentBuffer: Array<MutableMap<String, Any?>> = emptyArray()
    private var currentBufferIndex: Int
    private var totalColumnCount: Int
    private var fieldNames: List<String>

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
        currentBatch = orcReader.schema.createRowBatch()
        fieldNames = orcReader.schema.fieldNames
        currentBufferIndex = 0
        rows.nextBatch( currentBatch )
        totalColumnCount = currentBatch.numCols
        currentBuffer = mapBatchToRealTypes(currentBatch.size)
    }

    override fun hasNext(): Boolean {
        return !currentBatch.endOfFile
    }

    companion object {
        fun processList( columnVector: ColumnVector ): List<Any>{
            when (columnVector.type) {
                ColumnVector.Type.LONG -> {
                    return Lists.newArrayList( (columnVector as LongColumnVector).vector )
                }
                ColumnVector.Type.DOUBLE -> {
                    return Lists.newArrayList( (columnVector as DoubleColumnVector).vector )
                }
                ColumnVector.Type.BYTES -> {
                    return Lists.newArrayList( (columnVector as BytesColumnVector).vector )
                }
                ColumnVector.Type.DECIMAL -> {
                    return Lists.newArrayList( (columnVector as DecimalColumnVector).vector )
                }
                ColumnVector.Type.DECIMAL_64 -> {
                    return Lists.newArrayList( (columnVector as Decimal64ColumnVector).vector )
                }
                ColumnVector.Type.LIST -> {
                    return processList( (columnVector as ListColumnVector ))
                }
                ColumnVector.Type.TIMESTAMP,
                ColumnVector.Type.INTERVAL_DAY_TIME,
                ColumnVector.Type.STRUCT,
                ColumnVector.Type.MAP,
                ColumnVector.Type.UNION,
                ColumnVector.Type.VOID,
                ColumnVector.Type.NONE -> {
                }
            }
            return ImmutableList.of()
        }

        fun processColumnVectorType(columnVector: ColumnVector, newBufferSize: Int, columnName: String, buffer: Array<MutableMap<String, Any?>>){
            println("it a ${columnVector.type}")
            when (columnVector.type) {
                ColumnVector.Type.LONG -> {
                    val longVector = (columnVector as LongColumnVector).vector
                    for (rowIndex in 0 until newBufferSize) {
                        buffer[rowIndex].put(columnName, longVector[rowIndex])
                    }
                }
                ColumnVector.Type.DOUBLE -> {
                    val doubleVector = (columnVector as DoubleColumnVector).vector
                    for (rowIndex in 0 until newBufferSize) {
                        buffer[rowIndex].put(columnName, doubleVector[rowIndex])
                    }
                }
                ColumnVector.Type.BYTES -> {
                    val byteArrayVector = (columnVector as BytesColumnVector).vector
                    for (rowIndex in 0 until newBufferSize) {
                        buffer[rowIndex].put(columnName, byteArrayVector.get(rowIndex))
                    }
                }
                ColumnVector.Type.DECIMAL -> {
                    val decimalVector = (columnVector as DecimalColumnVector).vector
                    for (rowIndex in 0 until newBufferSize) {
                        buffer[rowIndex].put(columnName, decimalVector[rowIndex])
                    }
                }
                ColumnVector.Type.DECIMAL_64 -> {
                    val decimal64Vector = (columnVector as Decimal64ColumnVector).vector
                    for (rowIndex in 0 until newBufferSize) {
                        buffer[rowIndex].put(columnName, decimal64Vector[rowIndex])
                    }
                }
                ColumnVector.Type.LIST -> {
                    val vecList = (columnVector as ListColumnVector)
                    for (rowIndex in 0 until newBufferSize) {
                        buffer[rowIndex].put(columnName, processList( vecList.child ))
                    }
                }
                ColumnVector.Type.TIMESTAMP,
                ColumnVector.Type.INTERVAL_DAY_TIME,
                ColumnVector.Type.STRUCT,
                ColumnVector.Type.MAP,
                ColumnVector.Type.UNION,
                ColumnVector.Type.VOID,
                ColumnVector.Type.NONE -> {
                    for (rowIndex in 0 until newBufferSize) {
                        buffer[rowIndex].put(columnName, columnVector)
                    }
                }
                null -> {
                }
            }
        }
    }

    fun mapBatchToRealTypes(newBufferSize: Int): Array<MutableMap<String, Any?>> {
        val buffer = Array<MutableMap<String, Any?>>(newBufferSize, {
            Maps.newLinkedHashMapWithExpectedSize<String, Any>(totalColumnCount)
        })
        for (columnIndex in 0 until totalColumnCount) {
            val columnName = fieldNames.get(columnIndex)
            val columnVector = currentBatch.cols[columnIndex]
            processColumnVectorType(columnVector, newBufferSize, columnName, buffer)
        }
        return buffer
    }

    override fun next(): Map<String, Any?> {
        // end of buffer, refresh
        if ( currentBufferIndex == currentBuffer.size ) {
            rows.nextBatch( currentBatch )
            currentBuffer = mapBatchToRealTypes(currentBatch.size)
            currentBufferIndex = 0
        }
        return currentBuffer[currentBufferIndex++]
    }
}
