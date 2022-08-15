package com.openlattice.shuttle

import com.geekbeast.ResourceConfigurationLoader
import org.junit.Assert
import org.junit.Test
import org.slf4j.LoggerFactory

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@getmethodic.com&gt;
 */
class MissionParameterSerializerTest {
    companion object {
        private val logger = LoggerFactory.getLogger(MissionParameterSerializerTest::class.java)
    }
    @Test
    fun testLoadingMissionParameters() {
        val mp = ResourceConfigurationLoader.loadConfiguration(MissionParameters::class.java)
        Assert.assertTrue( mp.postgres.config.isNotEmpty() )
        Assert.assertNotNull( mp )
        logger.info("Mission Parameters: {}", mp)
    }
}