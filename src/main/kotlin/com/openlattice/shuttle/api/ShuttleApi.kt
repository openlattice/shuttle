package com.openlattice.shuttle.api

import com.openlattice.shuttle.control.Integration
import com.openlattice.shuttle.control.IntegrationUpdate
import retrofit2.http.*


const val SERVICE = "/shuttle"
const val CONTROLLER = "/integration"
const val BASE = SERVICE + CONTROLLER

const val DEFINITION_PATH = "/definition"
const val FLIGHT_PATH = "/flight"

const val INTEGRATION_NAME = "integrationName"
const val INTEGRATION_NAME_PATH = "/{$INTEGRATION_NAME}"

interface ShuttleApi {

    /**
     * Starts an integration on Shuttle Server for a given flight
     * @param integrationName the name of the integration to be run
     */
    @PATCH(BASE + INTEGRATION_NAME_PATH)
    fun startIntegration(
            @Path(INTEGRATION_NAME) integrationName: String
    )

    /**
     * Creates a new integration definition for running recurring integrations
     * @param integrationName the name of the integration definition to be created
     * @param integrationDefinition the definition of the integration. The path to
     * a flight yaml can be passed in place of a Flight object and will be converted
     * to an instance of Flight class
     */
    @POST(BASE + DEFINITION_PATH + INTEGRATION_NAME_PATH)
    fun createIntegrationDefinition(
            @Path(INTEGRATION_NAME) integrationName: String,
            @Body integrationDefinition: Integration )


    /**
     * Gets an existing integration definition
     * @param integrationName the name of the integration definition to get
     */
    @GET(BASE + DEFINITION_PATH + INTEGRATION_NAME_PATH)
    fun readIntegrationDefinition(
            @Path(INTEGRATION_NAME) integrationName: String
    ) : Integration

    /**
     * Replaces any number of fields within an existing integration definition
     * @param integrationName the name of the integration definition to be replaced
     * @param integrationUpdate the integration definition to replace an
     * existing one. Note, if a new flightFilePath is included in the update and the
     * contents of the yaml at that path have changed, the flight will be updated to
     * reflect these changes.
     */
    @PATCH(BASE + DEFINITION_PATH + INTEGRATION_NAME_PATH)
    fun updateIntegrationDefinition(
            @Path(INTEGRATION_NAME) integrationName: String,
            @Body integrationUpdate: IntegrationUpdate
    )

    /**
     * Reloads the flights within an integration definition from the paths that
     * are currently stored within the integration definition
     * @param integrationName the name of the integration definition to be reloaded
     */
    @PATCH(BASE + DEFINITION_PATH + FLIGHT_PATH + INTEGRATION_NAME_PATH)
    fun reloadFlightsWithinIntegrationDefinition(
            @Path(INTEGRATION_NAME) integrationName: String
    )

    /**
     * Deletes an integration definition
     * @param integrationName the name of the integration definition to be deleted
     */
    @DELETE(BASE + DEFINITION_PATH + INTEGRATION_NAME_PATH)
    fun deleteIntegrationDefinition(
            @Path(INTEGRATION_NAME) integrationName: String
    )

}