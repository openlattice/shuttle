package com.openlattice.shuttle.control

import com.dataloom.mappers.ObjectMappers
import com.openlattice.client.RetrofitFactory
import com.openlattice.data.integration.StorageDestination
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.shuttle.Flight
import java.sql.ResultSet
import java.util.*

class IntegrationResultSetAdapter {

    fun integration(rs: ResultSet): Integration {
        val name = rs.getString(NAME.name)
        val sql = rs.getString(SQL.name)
        //source somehow
        val src = Properties()
        val srcPkeyCols = PostgresArrays.getTextArray(rs, SRC_PKEY_COLUMNS_FIELD).toList()
        val environment = RetrofitFactory.Environment.valueOf(rs.getString(ENVIRONMENT.name).toUpperCase())
        val defaultStorage = StorageDestination.valueOf(rs.getString(DEFAULT_STORAGE.name).toUpperCase())
        val s3bucket = rs.getString(S3_BUCKET.name)
        val flight = buildFlight(rs)
        val recurring = rs.getBoolean(RECURRING.name)
        return Integration(name, sql, src, srcPkeyCols, environment, defaultStorage, s3bucket, flight, recurring)
    }

    private fun buildFlight(rs: ResultSet): Flight {
        val flightAsString = rs.getString(FLIGHT.name)
        return ObjectMappers.getYamlMapper().readValue(flightAsString, Flight::class.java)
    }
}