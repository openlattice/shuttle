package com.openlattice.shuttle.payload;

import com.dataloom.streams.StreamUtil;
import com.google.common.collect.ImmutableList;
import com.openlattice.shuttle.util.CsvUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.openlattice.shuttle.util.CsvUtil.newDefaultMapper;

public class FilterablePayload implements Payload {
    protected static final Logger logger = LoggerFactory.getLogger( FilterablePayload.class );

    private final List<Map<String, String>> payload;

    public FilterablePayload( String path ) {
        this.payload = StreamUtil.stream( () -> {
            try {
                return newDefaultMapper()
                        .readerFor( Map.class )
                        .with( CsvUtil.newDefaultSchemaFromHeader() )
                        .readValues( new File( path ) );
            } catch ( IOException e ) {
                logger.error( "Unable to read csv file", e );
                return ImmutableList.<Map<String, String>>of().iterator();
            }
        } ).collect( Collectors.toList() );
    }

    public SimplePayload filterPayload( Predicate<Map<String, String>>... filters ) {
        Predicate<Map<String, String>> filter = row -> true;
        for ( int i = 0; i < filters.length; i++ ) {
            filter = filter.and( filters[ i ] );
        }
        return new SimplePayload( payload.stream().filter( filter ) );
    }

    @Override
    public Stream<Map<String, String>> getPayload() {
        return payload.stream();
    }

    public static Predicate<Map<String, String>> equalPredicate( String colName, String value ) {
        return row -> row.get( colName ).equals( value );
    }

    public static Predicate<Map<String, String>> notEqualPredicate( String colName, String value ) {
        return row -> !row.get( colName ).equals( value );
    }

}
