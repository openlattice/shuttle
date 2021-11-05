package com.openlattice.shuttle

import com.openlattice.organizations.JdbcConnectionParameters
import com.openlattice.shuttle.config.ArchiveConfig
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.joda.time.Days
import org.slf4j.LoggerFactory

import org.springframework.stereotype.Service
import java.util.*
import java.time.temporal.ChronoUnit.DAYS
import java.sql.Connection
import java.sql.Statement
import java.time.LocalDate

/**
 * @author Andrew Carter andrew@openlattice.com
 */

//Export
//1. Accept parameters in shuttle cli
//2. Validate parameters
//3. Pass parameters to new class
//4. New class creates chunks by day or specified duration
//5. Ensure that s3 is reachable?
//6. Connect to atlas
//7. Execute query per day - updating file path and export query per step
//8. Check if succeeded per step
//9. Write finished chechpoint
//10. Validate write succeed by querying s3 to ensure row count matches (per step?)

private val logger = LoggerFactory.getLogger(ArchiveService::class.java)

@Service
class ArchiveService(
    private val archiveConfig: ArchiveConfig,
    private val dbName: String,
    private val schemaName: String = "openlattice",
    private val sourceName: String,
    private val destinationName: String = sourceName
) {
    init {
        logger.info("Initiating ArchiveService...")
    }

    fun mummify(startDate: LocalDate, days: Int) {
        logger.info("Beginning mummification...")
        connectAsSuperuser(dbName).use { connection ->
            connection.createStatement().use { statement ->
                generateAndExecuteSqlPerDay(statement, startDate, days, ::exportSql)
            }
        }
    }

    fun exhume(startDate: LocalDate, days: Int) {
        logger.info("Exhuming data...")
        connectAsSuperuser(dbName).use { connection ->
            connection.createStatement().use { statement ->
                generateAndExecuteSqlPerDay(statement, startDate, days, ::importSql)
            }
        }
    }

    fun generateAndExecuteSqlPerDay(
        statement: Statement,
        startDate: LocalDate,
        days: Int,
        sqlGenerator: (input: String) -> String
    ) {
        for (index in 0 until days) {
            val currentDate = startDate.plusDays(index.toLong()).toString()
            val sql = sqlGenerator(currentDate)
            logger.info("Executing query:\n $sql")
            if (statement.execute(sql)) {
                logger.info("Successfully executed query of $currentDate from $sourceName to $destinationName")
            }
        }
    }

    fun connectAsSuperuser(dbName: String): Connection {
        val config = HikariConfig(archiveConfig.hikariConfiguration)
        config.jdbcUrl = "${config.jdbcUrl.removeSuffix("/")}/$dbName"

        return HikariDataSource(config).connection
    }

    fun exportSql(
        date: String,
    ): String {
        return  "SELECT * FROM aws_s3.query_export_to_s3(" +
                "'SELECT * FROM $schemaName.$sourceName', " +
                "aws_commons.create_s3_uri(\n" +
                "   '${archiveConfig.s3Bucket},\n" +
                "   '$dbName/$schemaName/$destinationName-$date',\n" +
                "   '${archiveConfig.s3Region}'\n" +
                "));"
    }

    fun importSql(
        date: String,
    ): String {
        return " SELECT aws_s3.table_import_from_s3(" +
                "'$destinationName',\n" +
                "'', ''," +
                "aws_commons.create_s3_uri(\n" +
                "   '${archiveConfig.s3Bucket}',\n" +
                "   '$dbName/$schemaName/$sourceName-$date',\n" +
                "   '${archiveConfig.s3Region}'\n" +
                "));"
    }

}