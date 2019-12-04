package com.openlattice.shuttle.api

import com.openlattice.shuttle.control.Integration
import retrofit2.http.*
import java.util.*


const val SERVICE = "/shuttle"
const val CONTROLLER = "/integration"
const val BASE = SERVICE + CONTROLLER

const val DEFINITION = "/definition"

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
            @Path(FLIGHT_NAME) flightName: String
    )

    /**
     * Creates a new Integration definition for running recurring integrations
     * @param integrationDefinition the definition of the integration. The path to
     * a flight yaml can be passed in place of a Flight object and will be converted
     * to an instance of Flight class
     */
    @POST(BASE + DEFINITION + FLIGHT_NAME_PATH)
    fun createIntegrationDefinition(
            @Path(FLIGHT_NAME) flightName: String,
            @Body integrationDefinition: Integration )

    @PATCH(BASE + DEFINITION + FLIGHT_NAME_PATH)
    fun updateIntegrationDefinition(
            @Path(FLIGHT_NAME) flightName: String,
            @Body integrationDefinition: Integration
    )

    @DELETE(BASE + DEFINITION + FLIGHT_NAME_PATH)
    fun deleteIntegrationDefinition(
            @Path(FLIGHT_NAME) flightName: String
    )

}