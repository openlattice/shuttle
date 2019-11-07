package com.openlattice.shuttle.pods

import com.google.common.collect.Lists
import com.kryptnostic.rhizome.configuration.servlets.DispatcherServletConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
class ShuttleServletsPod {
    @Bean
    fun shuttleServlet(): DispatcherServletConfiguration {

        return DispatcherServletConfiguration(
                "shuttle",
                arrayOf("/shuttle/*"),
                1,
                listOf(ShuttleMvcPod::class.java)
        )
    }


}