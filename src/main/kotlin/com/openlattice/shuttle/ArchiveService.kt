package com.openlattice.shuttle

import com.openlattice.organizations.JdbcConnectionParameters
import com.openlattice.shuttle.config.ArchiveConfig
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.joda.time.Days
import org.postgresql.util.PSQLException
import org.slf4j.LoggerFactory

import org.springframework.stereotype.Service
import java.util.*
import java.time.temporal.ChronoUnit.DAYS
import java.sql.Connection
import java.sql.Statement
import java.time.LocalDate

const val DEFAULT_DAYS = 1

/**
 * @author Andrew Carter andrew@openlattice.com
 */

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

    // archives data
    fun mummify(startDate: LocalDate, days: Int) {
        logger.info("Beginning mummification...")
        connectToDatabase(dbName).use { connection ->
            connection.createStatement().use { statement ->
                generateAndExecuteSqlPerDay(statement, startDate, days, ::exportSql)
            }
        }
    }

    // restores data
    fun exhume(startDate: LocalDate, days: Int) {
        logger.info("Exhuming data...")
        connectToDatabase(dbName).use { connection ->
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
            try {
                statement.execute(sql)
                logger.info("Successfully executed query of $currentDate from $sourceName to $destinationName")
            } catch (e: PSQLException) {
                throw Error("Unsuccessful sql execution", e)
            }
        }
    }

    fun connectToDatabase(dbName: String): Connection {
        val config = HikariConfig(archiveConfig.hikariConfiguration)
        // append org database name to the jdbc url
        config.jdbcUrl = "${config.jdbcUrl.removeSuffix("/")}/$dbName"

        return HikariDataSource(config).connection
    }

    // generates sql to invoke an export using aws_s3 postgres extension
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

    // generates sql to invoke an import using aws_s3 postgres extension
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