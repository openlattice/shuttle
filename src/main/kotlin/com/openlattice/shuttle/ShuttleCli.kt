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

import org.apache.commons.cli.*
import kotlin.system.exitProcess

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class ShuttleCli {
    companion object {
        const val HELP = "help"

        const val FLIGHT = "flight"
        const val USER = "user"
        const val PASSWORD = "password"
        const val TOKEN = "token"
        const val CREATE = "create"
        const val ENVIRONMENT = "environment"
        const val SQL = "sql"
        const val CSV = "csv"
        const val DATASOURCE = "datasource"
        const val CONFIGURATION = "config"

        private val options = Options()
        private val clp = DefaultParser()
        private val hf = HelpFormatter()

        private val flightOption = Option.builder()
                .longOpt(FLIGHT)
                .desc("Attempt to load configuration from S3.")
                .hasArg(true)
                .argName("file")
                .build()

        private val environmentOption = Option.builder()
                .longOpt(ENVIRONMENT)
                .desc("Specifies an environment to run the integration against. Possible values are LOCAL or PRODUCTION")
                .required(false)
                .hasArg()
                .argName("environment")
                .build()

        private val configurationOption = Option.builder()
                .longOpt(CONFIGURATION)
                .desc("File containing integration configuration")
                .hasArg()
                .argName("file")
                .build()

        private val datasourceOption = Option.builder()
                .longOpt(DATASOURCE)
                .hasArg()
                .argName("Name of the source in the configuration file ")
                .build()

        private val userOption = Option.builder()
                .longOpt(USER)
                .desc("Creates any entity sets that are missing ")
                .hasArg(true)
                .argName("Auth0 username")
                .build()

        private val passwordOption = Option.builder()
                .longOpt(PASSWORD)
                .desc("Creates any entity sets that are missing ")
                .hasArg(true)
                .argName("password")
                .build()

        private val tokenOption = Option.builder()
                .longOpt(TOKEN)
                .desc("Creates any entity sets that are missing ")
                .hasArg(true)
                .argName("Auth0 JWT Token")
                .build()

        private val createOption = Option.builder()
                .longOpt(CREATE)
                .desc("Creates any entity sets that are missing ")
                .hasArg(false)
                .build()

        private val helpOption = Option.builder(HELP.first().toString())
                .longOpt(HELP)
                .desc("Print this help message.")
                .hasArg(false)
                .build()

        private val sqlOption = Option.builder()
                .longOpt(SQL)
                .desc("SQL query to use for the flight.")
                .argName("query")
                .hasArg(true)
                .build()

        private val csvOption = Option.builder()
                .longOpt(CSV)
                .desc("CSV file to use as the datasource for a specific flight.")
                .hasArg(true)
                .argName("file")
                .build()


        init {
            options.addOption(helpOption)
            options.addOption(configurationOption)
            options.addOption(datasourceOption)
            options.addOption(flightOption)
            options.addOption(environmentOption)
            options.addOption(tokenOption)
            options.addOption(userOption)
            options.addOption(passwordOption)
            options.addOption(createOption)

            options.addOptionGroup(
                    OptionGroup()
                            .addOption(sqlOption)
                            .addOption(csvOption)
            )

            options.addOptionGroup(
                    OptionGroup()
                            .addOption(userOption)
                            .addOption(tokenOption)
            )
        }

        @JvmStatic
        fun parseCommandLine(args: Array<String>): CommandLine {
            try {
                return clp.parse(options, args)
            } catch (ex: AlreadySelectedException) {
                System.out.println(ex.message)
                printHelp()
                exitProcess(1)
            }
        }

        fun printHelp() {
            hf.printHelp("shuttle", options)
        }
    }
}