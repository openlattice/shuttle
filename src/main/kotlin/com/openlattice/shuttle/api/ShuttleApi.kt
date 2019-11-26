package com.openlattice.shuttle.api

import retrofit2.http.Body
import retrofit2.http.PATCH
import retrofit2.http.Path


const val SERVICE = "/shuttle"
const val CONTROLLER = "/integration"
const val BASE = SERVICE + CONTROLLER

const val FLIGHT = "flight"
const val FLIGHT_PATH = "{$FLIGHT}"

interface ShuttleApi {

    /**
     * Starts an integration on Shuttle Server for a given flight
     * @param flightName the name of the flight to be run
     * @param lastRow the last row that has already been integrated
     */
    @PATCH(BASE + FLIGHT_PATH)
    fun startIntegration(
            @Path(FLIGHT) flightName: String,
            @Body lastRow: String //not accurate, will need something better once we know what we need
    )

}