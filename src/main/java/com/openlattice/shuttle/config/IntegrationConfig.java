package com.openlattice.shuttle.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class IntegrationConfig {
    private static final Logger                  logger = LoggerFactory.getLogger( IntegrationConfig.class );
    private final        Map<String, Properties> hikariConfigurations;
    private final        List<String>            primaryKeyColumns;

    @JsonCreator
    public IntegrationConfig(
            @JsonProperty( "primaryKeyColumns" )
                    Optional<List<String>> primaryKeyColumns,
            @JsonProperty( "hikariConfigs" )
                    Map<String, Properties> hikariConfigurations ) {
        this.hikariConfigurations = hikariConfigurations;
        this.primaryKeyColumns = primaryKeyColumns.orElse( List.of() );
    }

    @JsonProperty( "hikariConfigs" )
    public Map<String, Properties> getHikariConfigurations() {
        return hikariConfigurations;
    }

    @JsonIgnore
    public HikariDataSource getHikariDatasource( String name ) {
        Properties properties = checkNotNull( hikariConfigurations.get( name ), "Hikari configuration doesn't exit" );

        HikariConfig hc = new HikariConfig( properties );
        logger.info( "JDBC URL = {}", hc.getJdbcUrl() );
        return new HikariDataSource( hc );
    }

    @JsonProperty( "primaryKeyColumns" )
    public List<String> getPrimaryKeyColumns() {
        return primaryKeyColumns;
    }

}
