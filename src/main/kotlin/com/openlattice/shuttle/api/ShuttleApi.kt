package com.openlattice.shuttle.api

import com.openlattice.shuttle.control.Integration
import com.openlattice.shuttle.control.IntegrationJob
import com.openlattice.shuttle.control.IntegrationStatus
import com.openlattice.shuttle.control.IntegrationUpdate
import retrofit2.http.*
import java.util.*


const val SERVICE = "/shuttle"
const val CONTROLLER = "/integration"
const val BASE = SERVICE + CONTROLLER

const val DEFINITION_PATH = "/definition"
const val STATUS_PATH = "/status"

const val INTEGRATION_NAME = "integrationName"
const val INTEGRATION_NAME_PATH = "/{$INTEGRATION_NAME}"
const val INTEGRATION_KEY = "integrationKey"
const val INTEGRATION_KEY_PATH = "/{$INTEGRATION_KEY}"
const val JOB_ID = "jobId"
const val JOB_ID_PATH = "/{$JOB_ID}"
const val CALLBACK = "callbackUrl"
const val CALLBACK_PATH = "/{$CALLBACK}"

interface ShuttleApi {

    /**
     * Enqueues an integration on Shuttle Server for a given integration
     * @param integrationName the name of the integration to be run
     * @param integrationKey the unique id used to authenticate an integration run
     * @return the unique id of the integration job
     */
    @GET(BASE + INTEGRATION_NAME_PATH + INTEGRATION_KEY_PATH + CALLBACK_PATH)
    fun enqueueIntegration(
            @Path(INTEGRATION_NAME) integrationName: String,
            @Path(INTEGRATION_KEY) integrationKey: UUID,
            @Path(CALLBACK) callbackUrl: Optional<String>
    ): UUID

    /**
     * Polls the status of an integration
     * @param jobId the unique id of the integration job
     * @return the status of the integration
     * Note, upon retrieving a final status (i.e. succeeded or failed),
     * the id/status will be removed from memory
     */
    @GET(BASE + STATUS_PATH + JOB_ID_PATH)
    fun pollIntegration(
            @Path(JOB_ID) jobId: UUID
    ): IntegrationStatus

    /**
     * Polls the statuses of all running integrations
     */
    @GET(BASE + STATUS_PATH)
    fun pollAllIntegrations(): Map<UUID, IntegrationJob>

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
            @Body integrationDefinition: Integration ) : UUID


    /**
     * Gets an existing integration definition
     * @param integrationName the name of the integration definition to get
     * @return the integration definition
     */
    @GET(BASE + DEFINITION_PATH + INTEGRATION_NAME_PATH)
    fun readIntegrationDefinition(
            @Path(INTEGRATION_NAME) integrationName: String
    ) : Integration

    /**
     * Replaces any number of fields within an existing integration definition
     * @param integrationName the name of the integration definition to be replaced
     * @param integrationUpdate the integration definition to replace an
     * existing one.
     */
    @PATCH(BASE + DEFINITION_PATH + INTEGRATION_NAME_PATH)
    fun updateIntegrationDefinition(
            @Path(INTEGRATION_NAME) integrationName: String,
            @Body integrationUpdate: IntegrationUpdate
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