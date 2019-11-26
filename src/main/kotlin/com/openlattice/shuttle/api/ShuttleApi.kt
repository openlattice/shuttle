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

    @PATCH(BASE + FLIGHT_PATH)
    fun startIntegration(
            @Path(FLIGHT) flightName: String,
            @Body lastRow: String //not accurate, will need something better once we know what we need
    )

}