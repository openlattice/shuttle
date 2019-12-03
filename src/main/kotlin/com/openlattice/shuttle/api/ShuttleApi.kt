package com.openlattice.shuttle.api

import retrofit2.http.Body
import retrofit2.http.PATCH
import retrofit2.http.Path
import java.util.*


const val SERVICE = "/shuttle"
const val CONTROLLER = "/integration"
const val BASE = SERVICE + CONTROLLER

const val FLIGHT_NAME = "flight-name"
const val FLIGHT_NAME_PATH = "{$FLIGHT_NAME}"

interface ShuttleApi {

    /**
     * Starts an integration on Shuttle Server for a given flight
     * @param entityKeyId the entity key id of the flight to be run (within the flights entity set)
     * @param lastRow the last row that has already been integrated
     */
    @PATCH(BASE + FLIGHT_NAME_PATH)
    fun startIntegration(
            @Path(FLIGHT_NAME) flightName: String,
            @Body lastRow: String //not accurate, will need something better once we know what we need
    )

}