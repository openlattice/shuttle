package com.openlattice.shuttle.api

import com.openlattice.shuttle.control.Integration
import retrofit2.http.*


const val SERVICE = "/shuttle"
const val CONTROLLER = "/integration"
const val BASE = SERVICE + CONTROLLER

const val DEFINITION = "/definition"

const val INTEGRATION_NAME = "integration-name"
const val INTEGRATION_NAME_PATH = "{$INTEGRATION_NAME}"

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
    @POST(BASE + DEFINITION + INTEGRATION_NAME_PATH)
    fun createIntegrationDefinition(
            @Path(INTEGRATION_NAME) integrationName: String,
            @Body integrationDefinition: Integration )

    @GET(BASE + DEFINITION + INTEGRATION_NAME_PATH)
    fun readIntegrationDefinition(
            @Path(INTEGRATION_NAME) integrationName: String
    ) : Integration

    /**
     * Replaces an existing integration definition
     * @param integrationName the name of the integration definition to be replaced
     * @param integrationDefinition the integration definition to replace an
     * existing one. The path to a flight yaml can be passed in place of a
     * Flight object and will be converted to an instance of Flight class
     */
    @PATCH(BASE + DEFINITION + INTEGRATION_NAME_PATH)
    fun updateIntegrationDefinition(
            @Path(INTEGRATION_NAME) integrationName: String,
            @Body integrationDefinition: Integration
    )

    /**
     * Deleted an integration definition
     * @param integrationName the name of the integration definition to be deleted
     */
    @DELETE(BASE + DEFINITION + INTEGRATION_NAME_PATH)
    fun deleteIntegrationDefinition(
            @Path(INTEGRATION_NAME) integrationName: String
    )

}