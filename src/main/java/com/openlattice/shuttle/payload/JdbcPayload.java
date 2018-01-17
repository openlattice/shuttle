package com.openlattice.shuttle.payload;

import com.dataloom.streams.StreamUtil;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class JdbcPayload implements Payload {
    private static final Logger logger = LoggerFactory.getLogger( JdbcPayload.class );
    private final HikariDataSource hds;
    private final String           sql;

    public JdbcPayload( HikariDataSource hds, String sql ) {
        this.hds = hds;
        this.sql = sql;
    }

    @Override public Stream<Map<String, String>> getPayload() {
        try ( Connection conn = hds.getConnection(); Statement statement = conn.createStatement() ) {
            final ResultSet rs = statement.executeQuery( sql );
            return StreamUtil.stream( () -> new ResultSetStringIterator( rs ) );
        } catch ( SQLException e ) {
            logger.info( "Unable to get payload.", e );
        }
        return null;
    }

    static final class ResultSetStringIterator implements Iterator<Map<String, String>> {
        private final ResultSet    rs;
        private final List<String> columns;
        private final int          columnCount;
        private AtomicBoolean hasNext = new AtomicBoolean( false );

        public ResultSetStringIterator( ResultSet rs ) {
            this.rs = rs;
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
                throw new IllegalStateException( "Result Set Iterator initialization failed" );
            }
        }

        @Override public boolean hasNext() {
            return hasNext.get();
        }

        @Override public Map<String, String> next() {
            Map<String, String> data = read( columns, rs );
            try {
                boolean hn = rs.next();
                hasNext.set( hn );
                if ( !hn ) {
                    rs.close();
                }
            } catch ( SQLException e ) {
                logger.error( "Unabe to advance to next item.", e );
            }
            return data;
        }

        private static Map<String, String> read( List<String> columns, ResultSet rs ) {
            return columns.stream().collect( Collectors.toMap( Function.identity(), col -> {
                String val = null;
                try {
                    val = (String) rs.getObject( col ).toString();
                } catch ( SQLException e ) {
                    logger.error( "Unabe to read col {}.", col, e );
                }
                return val;
            } ) );
        }
    }

    static final class ResultSetIterator implements Iterator<Map<String, Object>> {
        private final ResultSet    rs;
        private final List<String> columns;
        private final int          columnCount;
        private AtomicBoolean hasNext = new AtomicBoolean( false );

        public ResultSetIterator( ResultSet rs ) {
            this.rs = rs;
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
                throw new IllegalStateException( "Result Set Iterator initialization failed" );
            }
        }

        @Override public boolean hasNext() {
            return hasNext.get();
        }

        @Override public Map<String, Object> next() {
            Map<String, Object> data = read( columns, rs );
            try {
                boolean hn = rs.next();
                hasNext.set( hn );
                if ( !hn ) {
                    rs.close();
                }
            } catch ( SQLException e ) {
                logger.error( "Unabe to advance to next item.", e );
            }
            return data;
        }

        private static Map<String, Object> read( List<String> columns, ResultSet rs ) {
            return columns.stream().collect( Collectors.toMap( Function.identity(), col -> {
                try {
                    return rs.getObject( col );
                } catch ( SQLException e ) {
                    logger.error( "Unabe to read col {}.", col, e );
                    return null;
                }
            } ) );
        }
    }

}
