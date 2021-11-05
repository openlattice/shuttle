package com.openlattice.shuttle.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Andrew Carter andrew@openlattice.com
 *
 * Contains connection details (jdbc and s3) for archiving.
 */
public class ArchiveConfig {
    private static final Logger logger = LoggerFactory.getLogger( ArchiveYamlMapping.class );
    private final Properties      hikariConfiguration;
    private final String          s3Bucket;
    private final String          s3Region;

    ArchiveConfig(
            @JsonProperty( "hikariConfig" ) Properties hikariConfiguration,
            @JsonProperty( "s3Bucket" )             String s3Bucket,
            @JsonProperty( "s3Region" )             String s3Region
    ) {
        this.hikariConfiguration = hikariConfiguration;
        this.s3Bucket = s3Bucket;
        this.s3Region = s3Region;
    }

    @JsonProperty( "hikariConfig" )
    public Properties getHikariConfiguration() { return hikariConfiguration; }

    @JsonProperty( "s3Bucket" )
    public String getS3Bucket() { return s3Bucket; }

    @JsonProperty( "s3Region" )
    public String getS3Region() { return s3Region; }

}
