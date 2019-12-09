package com.openlattice.shuttle.controllers

import com.openlattice.shuttle.control.IntegrationsService
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping("/integrations")
class IntegrationsContoller {
    @Inject
    private lateinit var integrations: IntegrationsService
//
//    @GetMapping(value = ["","/"])
//    fun getIntegrations() : Map<String, Integration> {
//        return integrations.getIntegrations();
//    }
//
//    @PostMapping(value = ["","/"],consumes = MediaType.APPLICATION_JSON_VALUE)
//    fun createIntegration( integration: Integration) {}
}