package com.openlattice.shuttle.pods

import com.geekbeast.rhizome.configuration.servlets.DispatcherServletConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
class ShuttleServletsPod {
    @Bean
    fun shuttleServlet() = DispatcherServletConfiguration(
            "shuttle",
            arrayOf("/shuttle/*"),
            1,
            listOf(ShuttleMvcPod::class.java)
    )


}