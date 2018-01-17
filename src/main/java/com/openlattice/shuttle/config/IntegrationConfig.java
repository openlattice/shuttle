package com.openlattice.shuttle.config;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class IntegrationConfig {
    private static final Logger logger = LoggerFactory.getLogger( IntegrationConfig.class );
    private final Map<String, Properties> hikariConfigurations;

    public IntegrationConfig( Map<String, Properties> hikariConfigurations ) {
        this.hikariConfigurations = hikariConfigurations;
    }

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
}
