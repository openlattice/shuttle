package com.openlattice.shuttle.payload;

import com.dataloom.streams.StreamUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.openlattice.shuttle.util.CsvUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

import static com.openlattice.shuttle.util.CsvUtil.newDefaultMapper;

public class SimplePayload implements Payload {
    protected static final Logger logger = LoggerFactory.getLogger( SimplePayload.class );

    private final String path;

    public SimplePayload( String path ) {
        this.path = path;
    }

    @Override
    public Stream<Map<String, Object>> getPayload() {
        return StreamUtil.stream( () -> {
            try {
                return newDefaultMapper()
                        .readerFor( Map.class )
                        .with( CsvUtil.newDefaultSchemaFromHeader() )
                        .readValues( new File( path ) );
            } catch ( IOException e ) {
                logger.error( "Unable to read csv file", e );
                return ImmutableList.<Map<String, Object>>of().iterator();
            }
        } );
    }
}
