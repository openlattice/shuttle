package com.openlattice.shuttle

import com.amazonaws.SdkClientException
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ListObjectsRequest
import com.amazonaws.services.s3.model.ObjectListing
import com.openlattice.shuttle.config.ArchiveConfig
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.postgresql.util.PSQLException
import org.slf4j.LoggerFactory

import org.springframework.stereotype.Service
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.time.LocalDate

const val DEFAULT_DAYS = 1
const val NO_START_DATE = ""
const val S3_DELIMITER = "/"
const val S3_MARKER = ""
const val S3_MAX_KEYS = 1000

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
    private val destinationName: String = sourceName,
    private val dateField: String
) {
    private val s3Client: AmazonS3

    init {
        logger.info("Initiating ArchiveService...")
        s3Client = AmazonS3ClientBuilder.standard().withCredentials(
            AWSStaticCredentialsProvider(
                BasicAWSCredentials (
                    archiveConfig.accessKey,
                    archiveConfig.secretKey
                    )
            )
        ).withRegion(RegionUtils.getRegion(archiveConfig.s3Region).name).build()
    }

    // archive data
    // overwrite if already exists
    fun mummify(startDate: String, days: Int) {
        logger.info("Beginning mummification...")
        connectToDatabase(dbName).use { connection ->
            connection.createStatement().use { statement ->
                generateAndExecuteSqlPerDay(statement, startDate, days, ::exportHandler)
            }
        }
    }

    // restore data
    fun exhume(startDate: String, days: Int) {
        logger.info("Exhuming data...")
        connectToDatabase(dbName).use { connection ->
            connection.createStatement().use { statement ->
                generateAndExecuteSqlPerDay(statement, startDate, days, ::importHandler)
            }
        }
    }

    private fun generateAndExecuteSqlPerDay(
        statement: Statement,
        date: String,
        days: Int,
        sqlAndExecuteHandler: (statement: Statement, date: String) -> Unit
    ) {
        if (date == NO_START_DATE) {
            // if start date not provided, pass empty string
            sqlAndExecuteHandler(statement, NO_START_DATE)
        } else {
            // convert to date to LocalDate for date arithmetic
            val startDate = LocalDate.parse(date)
            for (dayIndex in 0 until days) {
                val currentDate = startDate.plusDays(dayIndex.toLong()).toString()
                sqlAndExecuteHandler(statement, currentDate)
            }
        }
    }

    private fun exportHandler(statement: Statement, currentDate: String) {
        isOverwrite(currentDate)
        executeStatement(statement, exportSql(currentDate))
        validateExport(currentDate)
    }

    private fun importHandler(statement: Statement, currentDate: String) {
        val parts = countOfS3ObjectsWithPrefix(currentDate, ::sourcePrefix)
        logger.info("Number of objects in s3 with prefix ${sourcePrefix(currentDate)}: $parts")
        for(part in 0 until parts) {
            // +1 to part to account for 0 vs 1 indexing
            executeStatement(statement, importSql(currentDate, part + 1))
        }
        validateImport(statement,currentDate)
    }

    // generate sql to invoke an export using aws_s3 postgres extension
    private fun exportSql(
        date: String,
    ): String {

        // avoid quoting hell in Postgres by using dollar-sign quotes ($exportQuery$)
        return  "SELECT * FROM aws_s3.query_export_to_s3(" +
                "\$exportQuery\$ " +
                "SELECT * FROM $schemaName.$sourceName " +
                whereClauseByDate(date) +
                " \$exportQuery\$," +
                "aws_commons.create_s3_uri(\n" +
                "   '${archiveConfig.s3Bucket}',\n" +
                "   '${destinationPrefix(date)}',\n" +
                "   '${archiveConfig.s3Region}'\n" +
                "));"
    }

    // generate sql to invoke an import using aws_s3 postgres extension
    private fun importSql(
        date: String,
        part: Int
    ): String {
        val partString = if (part > 1) "_part$part" else ""
        return " SELECT aws_s3.table_import_from_s3(" +
                "'$destinationName',\n" +
                "'', ''," +
                "aws_commons.create_s3_uri(\n" +
                "   '${archiveConfig.s3Bucket}',\n" +
                "   '${sourcePrefix(date)}$partString',\n" +
                "   '${archiveConfig.s3Region}'\n" +
                "));"
    }

    private fun validateExport(date: String) {
        if (countOfS3ObjectsWithPrefix(date, ::destinationPrefix) > 0) {
            logger.info("Export validation succeeded. Data written to s3.")
        } else {
            logger.error("Export validation failed: no data written was written to s3. " +
                    "Either there was a problem exporting or there is no data in the source table.")
        }
    }

    private fun validateImport(statement: Statement, date: String) {
        val query = "SELECT count(*) count " +
                "FROM $destinationName " +
                "${whereClauseByDate(date)};"

        val resultSet = executeStatement(statement, query)
        resultSet.next()
        val numRowsWritten = resultSet.getInt(1)

        if (numRowsWritten > 0) {
            logger.info("Import validation succeeded. $numRowsWritten rows found of $destinationName $date.")
        } else {
            logger.error("Import validation failed: no data written was written to $destinationName. " +
                    "Either there was a problem importing or there is no data in the source.")
        }
    }

    private fun isOverwrite(date: String) {
        val count = countOfS3ObjectsWithPrefix(date, ::destinationPrefix)
        if (count > 0) {
            logger.info("Overwriting. Number of objects in s3 with prefix ${destinationPrefix(date)}: $count")
        } else {
            logger.info("Creating new objects. No objects exist in s3 with prefix ${destinationPrefix(date)}")
        }
    }

    private fun countOfS3ObjectsWithPrefix(
        date: String,
        prefix: (date: String) -> String
    ): Int {
        val objects: ObjectListing
        val objectsRequest = ListObjectsRequest(
            archiveConfig.s3Bucket,
            prefix(date),
            S3_MARKER,
            S3_DELIMITER,
            S3_MAX_KEYS
        )
        try {
            objects = s3Client.listObjects(objectsRequest)
        } catch (e: SdkClientException) {
            throw Exception(e)
        }
        if (objects.isTruncated) {
            // TODO: Provide support for truncated / paginated result
            throw Exception("Too many objects with prefix ${destinationPrefix(date)}. Truncated ObjectListing not supported.")
        }
        return objects.objectSummaries.size
    }

    private fun destinationPrefix(date: String): String {
        return "archive01/$dbName/$schemaName/$destinationName${dateSuffix(date)}"
    }

    private fun sourcePrefix(date: String): String {
        return "archive01/$dbName/$schemaName/$sourceName${dateSuffix(date)}"
    }

    private fun dateSuffix(date: String): String {
        return if (date == NO_START_DATE) {
            ""
        } else {
            "/${sourceName}_$date"
        }
    }

    // if date not provided then don't include WHERE clause
    private fun whereClauseByDate(date: String): String {
        return if (date == NO_START_DATE) {
            ""
        } else {
            "WHERE DATE($dateField) = '$date'"
        }
    }

    private fun connectToDatabase(dbName: String): Connection {
        val config = HikariConfig(archiveConfig.hikariConfiguration)
        // append org database name to the jdbc url
        config.jdbcUrl = "${config.jdbcUrl.removeSuffix("/")}/$dbName"
        try {
            return HikariDataSource(config).connection
        } catch(e: Exception) {
            throw Error("Error connecting with HikariDataSource...", e)
        }
    }

    fun executeStatement(statement: Statement, sql: String): ResultSet {
        logger.info("Executing query:\n $sql")
        try {
            val rs = statement.executeQuery(sql)
            logger.info("Successfully executed query.\n")
            return rs
        } catch (e: PSQLException) {
            throw Error("Unsuccessful execution of sql $sql", e)
        }
    }
}