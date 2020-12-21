package com.openlattice.shuttle.pods

import com.amazonaws.services.s3.AmazonS3
import com.kryptnostic.rhizome.configuration.ConfigurationConstants
import com.kryptnostic.rhizome.configuration.amazon.AmazonLaunchConfiguration
import com.openlattice.ResourceConfigurationLoader
import com.openlattice.shuttle.MissionParameters
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

private val logger = LoggerFactory.getLogger(MissionParametersPod::class.java)

@Configuration
class MissionParametersPod {

    @Autowired(required = false)
    private lateinit var awsS3: AmazonS3

    @Autowired(required = false)
    private lateinit var awsLaunchConfig: AmazonLaunchConfiguration

    @Bean(name = ["missionParametersConfiguration"])
    @Profile(ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE)
    fun localMissionParameterConfiguration(): MissionParameters {
        val config = ResourceConfigurationLoader.loadConfiguration(MissionParameters::class.java)
        logger.info("Using local shuttle server configuration $config")
        return config
    }

    @Bean(name = ["missionParametersConfiguration"])
    @Profile(ConfigurationConstants.Profiles.AWS_TESTING_PROFILE,
            ConfigurationConstants.Profiles.AWS_CONFIGURATION_PROFILE)
    fun awsMissionParameterConfiguration(): MissionParameters {
        val alc = awsLaunchConfig!!
        val config = ResourceConfigurationLoader.loadConfigurationFromS3(
                awsS3,
                alc.bucket,
                alc.folder,
                MissionParameters::class.java
        )
        logger.info("Using aws shuttle server configuration $config")
        return config
    }

}