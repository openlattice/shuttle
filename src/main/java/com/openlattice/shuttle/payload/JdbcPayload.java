package com.openlattice.shuttle.payload;

import com.dataloom.streams.StreamUtil;
import com.google.common.util.concurrent.RateLimiter;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class JdbcPayload implements Payload {
    private static final Logger logger                     = LoggerFactory.getLogger( JdbcPayload.class );
    private static final int    DEFAULT_FETCH_SIZE         = 50000;
    private static final double DEFAULT_PERMITS_PER_SECOND = 10000.0;

    private final HikariDataSource hds;
    private final String           sql;
    private final RateLimiter      rateLimiter;
    private final int              fetchSize;
    private final List<String>     primaryKeyCols;

    public JdbcPayload( HikariDataSource hds, String sql ) {
        this( hds, sql, DEFAULT_PERMITS_PER_SECOND, DEFAULT_FETCH_SIZE );
    }

    public JdbcPayload( HikariDataSource hds, String sql, int fetchSize ) {
        this( hds, sql, DEFAULT_PERMITS_PER_SECOND, fetchSize );
    }

    public JdbcPayload( HikariDataSource hds, String sql, double permitsPerSecond, int fetchSize ) {
        this(hds, sql, permitsPerSecond, fetchSize, List.of());
    }

    public JdbcPayload( HikariDataSource hds, String sql, double permitsPerSecond, int fetchSize, List<String> primaryKeyCols ) {
        if ( primaryKeyCols == null || primaryKeyCols.isEmpty() ) {
            this.primaryKeyCols = List.of();
        } else {
            this.primaryKeyCols = primaryKeyCols;
        }
        this.rateLimiter = RateLimiter.create( permitsPerSecond );
        this.hds = hds;
        this.sql = sql;
        this.fetchSize = fetchSize;
    }

    @Override public Stream<Map<String, Object>> getPayload() {
        try {
            Connection conn = hds.getConnection();
            conn.setAutoCommit( false );
            Statement statement = conn.createStatement( ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY );
            statement.setFetchSize( fetchSize );
            final ResultSet rs = statement.executeQuery( sql );
            return StreamUtil.stream( () -> new ResultSetStringIterator( conn, statement, rs, rateLimiter, sql
                    , primaryKeyCols ) );
        } catch ( SQLException e ) {
            logger.info( "Unable to get payload.", e );
        }
        return null;
    }

    static final class ResultSetStringIterator implements Iterator<Map<String, Object>> {
        private static long READ_COUNT_THRESHOLD = 100000L;

        private final Connection    connection;
        private final Statement     stmt;
        private final ResultSet     rs;
        private final List<String>  columns;
        private final int           columnCount;
        private final RateLimiter   rateLimiter;
        private final String        sql;
        private final AtomicLong    readCount = new AtomicLong( 0 );
        private final List<String>  primaryKeyCols;
        private       AtomicBoolean hasNext   = new AtomicBoolean( false );

        public ResultSetStringIterator(
                Connection connection,
                Statement stmt,
                ResultSet rs,
                RateLimiter rateLimiter,
                String sql, List<String> primaryKeyCols ) {
            this.connection = connection;
            this.stmt = stmt;
            this.rs = rs;
            this.rateLimiter = rateLimiter;
            this.sql = sql;
            this.primaryKeyCols = primaryKeyCols;
            ResultSetMetaData rsm = null;
            try {
                rsm = rs.getMetaData();
                columnCount = rsm.getColumnCount();
                columns = new ArrayList( columnCount );
                for ( int i = 1; i <= columnCount; ++i ) {
                    columns.add( rsm.getColumnName( i ) );
                }
                hasNext.set( rs.next() );
            } catch ( SQLException e ) {
                throw new IllegalStateException( "ResultSummary Set Iterator initialization failed" );
            }
        }

        @Override public boolean hasNext() {
            return hasNext.get();
        }

        @Override public Map<String, Object> next() {
            rateLimiter.acquire();
            Map<String, Object> data = read( columns, rs );
            try {
                boolean hn = rs.next();
                hasNext.set( hn );
                if ( !hn ) {
                    rs.close();
                    stmt.close();
                    connection.close();
                    if (!primaryKeyCols.isEmpty()) {
                        logger.info("Final row read:");
                    }
                    for ( String column : primaryKeyCols ) {
                        logger.info( column + ": " + data.get( column ) );
                    }
                    logger.info( "Exhausted query {} after {} rows", sql, readCount.get() );
                }
            } catch ( SQLException e ) {
                logger.error( "Unable to advance to next item.", e );
            }
            final var currentReadCount = readCount.incrementAndGet();
            if ( ( currentReadCount % READ_COUNT_THRESHOLD ) == 0 ) {
                logger.info( "{} rows have been read for the query {}", currentReadCount, sql );
            }
            return data;
        }

        private static Map<String, Object> read( List<String> columns, ResultSet rs ) {
            return columns.stream().collect( Collectors.toMap( Function.identity(), col -> {
                Object val = "";
                try {
                    Object obj = rs.getObject( col );
                    if ( obj instanceof byte[] ) {
                        val = obj;
                    } else if ( obj != null ) { val = obj.toString(); }
                } catch ( SQLException e ) {
                    logger.error( "Unable to read col {}.", col, e );
                }
                return val;
            } ) );
        }
    }
}
