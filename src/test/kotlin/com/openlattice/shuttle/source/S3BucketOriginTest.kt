package com.openlattice.shuttle.source

import com.google.common.io.Resources
import com.openlattice.testing.util.S3TestingUtils
import org.junit.Before
import org.junit.Test
import java.io.InputStreamReader
import java.net.URL
import java.nio.file.Paths

class S3BucketOriginTest {

    private val TEST_S3_PORT = 9999
    private val TEST_REGION = "us-west-2"
    private val TEST_BUCKET = "testbucket"
    private val client = S3TestingUtils.newTestS3Client( TEST_S3_PORT, TEST_REGION )

    private val TEST_XML_FILE_PATH: URL = Resources.getResource( "xmls/xmlTest1.xml" )
    private val TEST_CSV_PATH: URL = Resources.getResource( "cyphers.csv" )
    private val xmlFileAsFile = Paths.get(TEST_XML_FILE_PATH.toURI()).toFile()
    private val csvFileAsFile =Paths.get(TEST_CSV_PATH.toURI()).toFile()

    @Before
    fun initialize() {
        client.createBucket(TEST_BUCKET);
        repeat( 10) {
            val nextFile = when ( it % 2 ) {
                0 -> xmlFileAsFile
                1 -> csvFileAsFile
                else -> xmlFileAsFile
            }
            client.putObject(TEST_BUCKET,"file/$it", nextFile);
        }
    }

    @Test
    fun testIterator() {
        var i = 0
        val s3BucketSource = S3BucketOrigin(TEST_BUCKET, client)
        s3BucketSource.map {
            Pair( InputStreamReader(it).readText() , i++)
        }.forEach {
            if ( it.second % 2 == 0 ) {
                assert(it.first == xmlFileAsFile.readText())
            } else {
                assert(it.first == csvFileAsFile.readText())
            }
        }
    }
}