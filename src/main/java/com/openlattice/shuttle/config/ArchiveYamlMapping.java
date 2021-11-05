package com.openlattice.shuttle.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.ArchiveService;

/**
 * @author Andrew Carter andrew@openlattice.com
 */
public class ArchiveYamlMapping {
    private final ArchiveConfig     archiveConfig;
    private final String            dbName;
    private final String            schemaName;
    private final String            sourceName;
    private final String            destinationName;

    @JsonCreator
    public ArchiveYamlMapping(
        @JsonProperty( "archiveParameters" )    ArchiveConfig archiveConfig,
        @JsonProperty( "dbName" )               String dbName,
        @JsonProperty( "schemaName" )           String schemaName,
        @JsonProperty( "sourceName" )           String sourceName,
        @JsonProperty( "destinationName" )      String destinationName
    ) {
        this.archiveConfig = archiveConfig;
        this.dbName = dbName;
        this.schemaName = schemaName;
        this.sourceName = sourceName;
        this.destinationName = destinationName;
    }

    @JsonProperty( "archiveConfig" )
    public ArchiveConfig getArchiveConfig() { return archiveConfig; }

    @JsonProperty( "dbName" )
    public String getDbName() { return dbName; }

    @JsonProperty("schemaName")
    public String getSchemaName() { return schemaName; }

    @JsonProperty( "sourceName" )
    public String getSourceName() { return sourceName; }

    @JsonProperty( "destinationName" )
    public String getDestinationName() { return destinationName; }
}
