package com.openlattice.shuttle.payload

import com.google.common.io.Resources
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.net.URL

internal class XmlFilesPayloadTest {

    private val TEST_PATH_1: URL = Resources.getResource( "xmls/xmltest1" )
    private val TEST_PATH_2: URL = Resources.getResource( "xmls/xmltest2" )
    private val TEST_PATH_3: URL = Resources.getResource( "xmls/" )
    private val TEST_PATH_4: URL = Resources.getResource( "xmls/xmltest1/xmlTest1.xml" )
    private val TEST_XML_READ: URL = Resources.getResource( "xmlDeepTest/Read_Example.xml" )
    private val TEST_XML_LPRD_TREE: URL = Resources.getResource( "xmlDeepTest/LPRD_XML_Detection_Example.xml" )

    private val OVERVIEW_IMG_TAG = "OverviewImage"
    private val EXPECTED_READ_KEYS = setOf("VehicleID", "Plate", "Date", "Time", "SharpName", "PlateImage", "ContextImage",
            OVERVIEW_IMG_TAG, "IsHit", "LatitudeDMS", "LongitudeDMS", "LatitudeDegree", "LatitudeMinute", "LatitudeSecond",
            "LongitudeDegree", "LongitudeMinute", "LongitudeSecond", "InventoryLocation", "State")
    private val EXPECTED_LPRD_KEYS = setOf( "EventDate", "EventTime", "LPRSystemID", "ActivityReason",
            "LPREventID", "OrganizationORIID.ID", "LPRVehicle.VehicleMakeCode", "LPRVehicle.VehicleModelCode",
            "LPRVehicle.VehicleModelYearDate", "LPRVehicle.VehicleModelCodeText", "LPRVehicle.VehicleColorPrimaryText",
            "LPRVehicle.VehicleColorPrimaryCode", "LPRVehicle.VehicleColorSecondaryText", "LPRVehicle.VehicleColorSecondaryCode",
            "LPRVehicle.VehicleDoorQuantity", "LPRVehicle.VehicleLicensePlateID.ID",
            "LPRVehicle.VehicleLicensePlateID.IDIssuingAuthorityText", "LPRVehiclePlatePhoto",
            "LPRGeographicCoordinate.GeographicCoordinateLatitude.LatitudeDegreeValue",
            "LPRGeographicCoordinate.GeographicCoordinateLatitude.LatitudeMinuteValue",
            "LPRGeographicCoordinate.GeographicCoordinateLatitude.LatitudeSecondValue",
            "LPRGeographicCoordinate.GeographicCoordinateLongitude.LongitudeDegreeValue",
            "LPRGeographicCoordinate.GeographicCoordinateLongitude.LongitudeMinuteValue",
            "LPRGeographicCoordinate.GeographicCoordinateLongitude.LongitudeSecondValue",
            "LPRMetadata.MetadataFieldName", "LPRMetadata.MetadataFieldValueText",
            "DocumentControlData.DocumentCountryCode.fips10-4", "LPRVehiclePlateTextCorrection",
            "LPRVehiclePlateStateCorrection", "LPRAdditionalPhoto", "LPRDirection", "LPRCameraID", "LPRRecordedLane")

    @Test
    fun testGetHappyPathPayload() {
        val xmlPayloadUnderTest = XmlFilesPayload(TEST_PATH_1.path)
        val count = basicTestFile( xmlPayloadUnderTest, EXPECTED_READ_KEYS, setOf( OVERVIEW_IMG_TAG ))
        assertTrue( count == 1 )
    }

    @Test
    fun testFilePathInsteadOfDirPath() {
        val xmlPayloadUnderTest = XmlFilesPayload(TEST_PATH_4.path)
        val count = basicTestFile( xmlPayloadUnderTest, EXPECTED_READ_KEYS, setOf( OVERVIEW_IMG_TAG ))
        assertTrue( count == 1 )
    }

    @Test
    fun testGetPayloadNestedDirectories() {
        val xmlPayloadUnderTest = XmlFilesPayload(TEST_PATH_3.path)
        val count = basicTestFile( xmlPayloadUnderTest, EXPECTED_READ_KEYS, setOf( OVERVIEW_IMG_TAG ))
        assertTrue( count == 1 )
    }

    @Test
    fun testGetPayloadMultipleFiles() {
        val xmlPayloadUnderTest = XmlFilesPayload(TEST_PATH_2.path)
        val count = basicTestFile( xmlPayloadUnderTest, EXPECTED_READ_KEYS, setOf( OVERVIEW_IMG_TAG ))
        assertTrue( count == 2 )
    }

    @Test
    fun testDeepXml() {
        val xmlPayloadUnderTest = XmlFilesPayload(TEST_XML_LPRD_TREE.path)
        val count = basicTestFile(xmlPayloadUnderTest, EXPECTED_LPRD_KEYS, setOf() )
        assertTrue( count == 1 )
    }

    @Test
    fun testShallowXml() {
        val xmlPayloadUnderTest = XmlFilesPayload(TEST_XML_READ.path)
        val count = basicTestFile( xmlPayloadUnderTest, EXPECTED_READ_KEYS, setOf( OVERVIEW_IMG_TAG ))
        assertTrue( count == 1 )
    }

    fun basicTestFile(xmlPayloadUnderTest: XmlFilesPayload, expectedKeys: Set<String>, absentKeys: Set<String> ): Int {
        var count = 0
        xmlPayloadUnderTest.getPayload().forEach {row ->
            count++
            //assert all keys present
            assertEquals(expectedKeys, row.keys)

            //assert any missing values dont have values in the map
            absentKeys.forEach {
                val emptyValue = row[it] as String
                assertTrue( emptyValue.isBlank() )
            }
        }
        return count
    }
}

