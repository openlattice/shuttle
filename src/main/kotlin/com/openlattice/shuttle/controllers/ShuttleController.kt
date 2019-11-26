package com.openlattice.shuttle.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.shuttle.*
import com.openlattice.shuttle.api.CONTROLLER
import com.openlattice.shuttle.api.FLIGHT
import com.openlattice.shuttle.api.ShuttleApi
import org.springframework.web.bind.annotation.*
import javax.inject.Inject

@RestController
@RequestMapping(CONTROLLER)
class ShuttleController : ShuttleApi {

    @Inject
    private lateinit var recurringIntegrationService: RecurringIntegrationService

    @Timed
    @PatchMapping(path = [FLIGHT])
    override fun startIntegration(
            @PathVariable(FLIGHT) flightName: String,
            @RequestBody lastRow: String
    ) {
        recurringIntegrationService.loadCargo(flightName, lastRow)
    }

}