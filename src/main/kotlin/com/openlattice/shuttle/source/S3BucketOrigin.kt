package com.openlattice.shuttle.source

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.iterable.S3Objects
import com.amazonaws.services.s3.model.GetObjectRequest
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.util.stream.StreamSupport


data class S3BucketOrigin(val bucketName: String, val s3Client: AmazonS3, val folderPrefix: String = "") : IntegrationOrigin() {

    companion object {
        val logger = LoggerFactory.getLogger(S3BucketOrigin::class.java)
    }

    override fun iterator(): Iterator<InputStream> {
        val inBucket = S3Objects.inBucket(s3Client, bucketName)
        val prefixPresent = folderPrefix != ""

        return StreamSupport.stream( inBucket.spliterator(), false ).filter{
            prefixPresent && it.key.startsWith(folderPrefix) && !it.key.endsWith('/')
        }.map {
            s3Client.getObject(GetObjectRequest(bucketName, it.key)).objectContent
        }.iterator()
    }

}