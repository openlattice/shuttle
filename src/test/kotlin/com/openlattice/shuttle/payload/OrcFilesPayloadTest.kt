package com.openlattice.shuttle.payload

import com.google.common.io.Resources
import org.junit.Assert
import org.junit.Test
import java.net.URL

internal class OrcFilesPayloadTest {

    private val TEST_PATH_1: URL = Resources.getResource( "orcs/test.orc" )

    @Test
    fun testGetHappyPathPayload() {
        val orcFilesPayloadUnderTest = OrcFilesPayload( TEST_PATH_1.path )
        var count = 0
        orcFilesPayloadUnderTest.getPayload().forEach {
            count++
        }
        Assert.assertTrue(count == 1)
    }

}
