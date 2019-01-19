package com.openlattice.shuttle.payload;

import static com.openlattice.shuttle.util.CsvUtil.newDefaultMapper;

import com.dataloom.streams.StreamUtil;
import com.google.common.collect.ImmutableList;
import com.openlattice.shuttle.util.CsvUtil;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterablePayload implements Payload {
    protected static final Logger logger = LoggerFactory.getLogger( FilterablePayload.class );

    private final String path;

    public FilterablePayload( String path ) {
        this.path = path;
    }

    public Payload filterPayload( Predicate<Map<String, String>>... filters ) {
        Predicate<Map<String, String>> filter = row -> true;
        for ( int i = 0; i < filters.length; i++ ) {
            filter = filter.and( filters[ i ] );
        }
        return new StreamPayload( getPayload().filter( filter ) );
    }

    @Override
    public Stream<Map<String, String>> getPayload() {
        return StreamUtil.stream( () -> {
            try {
                return newDefaultMapper()
                        .readerFor( Map.class )
                        .with( CsvUtil.newDefaultSchemaFromHeader() )
                        .readValues( new File( path ) );
            } catch ( IOException e ) {
                logger.error( "Unable to read csv file", e );
                return ImmutableList.<Map<String, String>>of().iterator();
            }
        } );
    }

    public static Predicate<Map<String, String>> equalPredicate( String colName, String value ) {
        return row -> row.get( colName ).equals( value );
    }

    public static Predicate<Map<String, String>> notEqualPredicate( String colName, String value ) {
        return row -> !row.get( colName ).equals( value );
    }

}
