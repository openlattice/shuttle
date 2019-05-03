package com.openlattice.shuttle.payload

import com.google.common.io.Resources
import org.junit.Assert.assertTrue
import org.junit.Test
import java.net.URL

internal class XmlPayloadTest {

    private val TEST_PATH_1: URL = Resources.getResource( "xmls/xmltest1" )
    private val TEST_PATH_2: URL = Resources.getResource( "xmls/xmltest2" )
    private val TEST_PATH_3: URL = Resources.getResource( "xmls/" )
    private val TEST_PATH_4: URL = Resources.getResource( "xmls/xmltest1/xmlTest1.xml" )

    private val OVERVIEW_IMG_TAG = "OverviewImage"
    private val EXPECTED_KEYS = setOf("VehicleID", "Plate", "Date", "Time", "SharpName", "PlateImage", "ContextImage",
            OVERVIEW_IMG_TAG, "IsHit", "LatitudeDMS", "LongitudeDMS", "LatitudeDegree", "LatitudeMinute", "LatitudeSecond",
            "LongitudeDegree", "LongitudeMinute", "LongitudeSecond", "InventoryLocation", "State")

    @Test
    fun testGetHappyPathPayload() {
        val xmlPayloadUnderTest = XmlPayload( TEST_PATH_1.path ).payload

        var count = 0
        xmlPayloadUnderTest.forEach {
            count++
            //assert all keys present
            assertTrue(EXPECTED_KEYS.equals(it.keys))

            //assert any missing values dont have values in the map
            val emptyValue = it[OVERVIEW_IMG_TAG] as String
            assertTrue( emptyValue.isBlank() )
            assertTrue( emptyValue.isEmpty() )
        }
        assertTrue( count == 1 )
    }

    @Test
    fun testFilePathInsteadOfDirPath() {
        val xmlPayloadUnderTest = XmlPayload( TEST_PATH_4.path ).payload

        var count = 0
        xmlPayloadUnderTest.forEach {
            count++
            //assert all keys present
            assertTrue(EXPECTED_KEYS.equals(it.keys))

            //assert any missing values dont have values in the map
            val emptyValue = it[OVERVIEW_IMG_TAG] as String
            assertTrue( emptyValue.isBlank() )
            assertTrue( emptyValue.isEmpty() )
        }
        assertTrue( count == 1 )
    }

    @Test
    fun testGetPayloadNestedDirectories() {
        val xmlPayloadUnderTest = XmlPayload( TEST_PATH_3.path ).payload
        var count = 0
        xmlPayloadUnderTest.forEach {
            count++
            //assert all keys present
            assertTrue(EXPECTED_KEYS.equals(it.keys))

            //assert any missing values dont have values in the map
            val emptyValue = it[OVERVIEW_IMG_TAG] as String
            assertTrue( emptyValue.isBlank() )
            assertTrue( emptyValue.isEmpty() )
        }
        assertTrue( count == 1 )
    }

    @Test
    fun testGetPayloadMultipleFiles() {
        val xmlPayloadUnderTest = XmlPayload( TEST_PATH_2.path ).payload
        var count = 0
        xmlPayloadUnderTest.forEach {
            count++
            //assert all keys present
            assertTrue(EXPECTED_KEYS.equals(it.keys))

            //assert any missing values dont have values in the map
            val emptyValue = it[OVERVIEW_IMG_TAG] as String
            assertTrue( emptyValue.isBlank() )
            assertTrue( emptyValue.isEmpty() )
        }
        assertTrue( count == 2 )

    }
}