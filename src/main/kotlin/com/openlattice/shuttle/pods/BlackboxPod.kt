package com.openlattice.shuttle.pods

import com.amazonaws.services.s3.AmazonS3
import com.hazelcast.core.HazelcastInstance
import com.geekbeast.rhizome.configuration.ConfigurationConstants
import com.geekbeast.rhizome.configuration.configuration.amazon.AmazonLaunchConfiguration
import com.geekbeast.ResourceConfigurationLoader
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer
import com.openlattice.shuttle.logs.Blackbox
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import javax.inject.Inject

private val logger = LoggerFactory.getLogger(BlackboxPod::class.java)

@Configuration
class BlackboxPod {

    init {
        FullQualifiedNameJacksonSerializer.registerWithMapper(ResourceConfigurationLoader.getYamlMapper())
    }

    @Inject
    private val hazelcastInstance: HazelcastInstance? = null

    @Autowired(required = false)
    private val awsS3: AmazonS3? = null

    @Autowired(required = false)
    private val awsLaunchConfig: AmazonLaunchConfiguration? = null

    @Bean(name = ["blackbox"])
    @Profile(ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE)
    fun getLocalBlackboxConfiguration(): Blackbox {
        val config = ResourceConfigurationLoader.loadConfiguration(Blackbox::class.java)
        logger.info("Using local blackbox configuration: {}", config)
        return config
    }

    @Bean(name = ["blackbox"])
    @Profile(
        ConfigurationConstants.Profiles.AWS_CONFIGURATION_PROFILE,
        ConfigurationConstants.Profiles.AWS_TESTING_PROFILE
    )
    fun getAwsBlackboxConfiguration(): Blackbox {
        val config = ResourceConfigurationLoader.loadConfigurationFromS3(
                awsS3!!,
                awsLaunchConfig!!.bucket,
                awsLaunchConfig.folder,
                Blackbox::class.java
        )
        logger.info("Using aws blackbox configuration: {}", config)
        return config
    }

}