/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.shuttle

import com.dataloom.mappers.ObjectMappers
import com.google.common.base.Preconditions
import com.openlattice.client.RetrofitFactory
import com.openlattice.shuttle.ShuttleCli.Companion.CONFIGURATION
import com.openlattice.shuttle.ShuttleCli.Companion.CREATE
import com.openlattice.shuttle.ShuttleCli.Companion.CSV
import com.openlattice.shuttle.ShuttleCli.Companion.DATASOURCE
import com.openlattice.shuttle.ShuttleCli.Companion.ENVIRONMENT
import com.openlattice.shuttle.ShuttleCli.Companion.FLIGHT
import com.openlattice.shuttle.ShuttleCli.Companion.HELP
import com.openlattice.shuttle.ShuttleCli.Companion.PASSWORD
import com.openlattice.shuttle.ShuttleCli.Companion.SQL
import com.openlattice.shuttle.ShuttleCli.Companion.TOKEN
import com.openlattice.shuttle.ShuttleCli.Companion.USER
import com.openlattice.shuttle.config.IntegrationConfig
import com.openlattice.shuttle.payload.JdbcPayload
import com.openlattice.shuttle.payload.Payload
import com.openlattice.shuttle.payload.SimplePayload
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.io.File

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

private val logger = LoggerFactory.getLogger(ShuttleCli::class.java)

fun main(args: Array<String>) {

    val configuration: IntegrationConfig
    val environment: RetrofitFactory.Environment
    val jwt: String
    val cl = ShuttleCli.parseCommandLine(args)
    val payload: Payload
    val flight: Flight
    val createEntitySets: Boolean
    val contacts: Set<String>


    if (cl.hasOption(HELP)) {
        ShuttleCli.printHelp()
        return
    }

    if (cl.hasOption(FLIGHT)) {
        flight = ObjectMappers.getYamlMapper().readValue(File(cl.getOptionValue(FLIGHT)), Flight::class.java)

    } else {
        System.err.println("A flight is required in order to run shuttle.")
        ShuttleCli.printHelp()
        return
    }

    //You can have a configuration without any JDBC datasrouces
    if (cl.hasOption(CONFIGURATION)) {
        configuration = ObjectMappers.getYamlMapper()
                .readValue(File(cl.getOptionValue(CONFIGURATION)), IntegrationConfig::class.java)

        if (!cl.hasOption(DATASOURCE)){
            // check datasource presence
            System.out.println("Datasource must be specified when doing a JDBC datasource based integration.")
            ShuttleCli.printHelp()
            return
        }
        if (!cl.hasOption(SQL)){
            // check SQL presence
            System.out.println("SQL expression must be specified when doing a JDBC datasource based integration.")
            ShuttleCli.printHelp()
            return
        }
        if (cl.hasOption(CSV)) {
            // check csv ABsence
            System.out.println("Cannot specify CSV datasource and JDBC datasource simultaneously.")
            ShuttleCli.printHelp()
            return
        }

        // get JDBC payload
        val hds = configuration.getHikariDatasource(cl.getOptionValue(DATASOURCE))
        val sql = cl.getOptionValue(SQL)
        payload = JdbcPayload(hds, sql)

    } else if (cl.hasOption(CSV)) {

        // get csv payload
        payload = SimplePayload(cl.getOptionValue(CSV))

    } else {
        System.err.println("At least one valid data source must be specified.")
        ShuttleCli.printHelp()
        return
    }


    if (cl.hasOption(ENVIRONMENT)) {
        environment = RetrofitFactory.Environment.valueOf(cl.getOptionValue(ENVIRONMENT).toUpperCase())
    } else {
        environment = RetrofitFactory.Environment.PRODUCTION
    }

    //TODO: Use the right method to select the JWT token for the appropriate environment.

    if (cl.hasOption(TOKEN)) {
        Preconditions.checkArgument(!cl.hasOption(PASSWORD))
        jwt = cl.getOptionValue(TOKEN)
    } else if (cl.hasOption(USER)) {
        Preconditions.checkArgument(cl.hasOption(PASSWORD))
        val user = cl.getOptionValue(USER)
        val password = cl.getOptionValue(PASSWORD)
        jwt = MissionControl.getIdToken(user, password)
    } else {
        System.err.println("User or token must be provided for authentication.")
        ShuttleCli.printHelp()
        return
    }

    createEntitySets = cl.hasOption(CREATE)
    if( createEntitySets ) {
        if( environment == RetrofitFactory.Environment.PRODUCTION ) {
            throw IllegalArgumentException( "It is not allowed to automatically create entity sets on " +
                    "${RetrofitFactory.Environment.PRODUCTION} environment" )
        }

        contacts = cl.getOptionValues( CREATE ).toSet()
        if( contacts.isEmpty() ) {
            throw IllegalArgumentException( "Can't create entity sets automatically without contacts provided" )
        }
    } else {
        contacts = setOf()
    }

    val flights = mapOf(flight to payload)

    val shuttle = Shuttle(environment, jwt)

    shuttle.launchPayloadFlight(flights, createEntitySets, contacts)
}

