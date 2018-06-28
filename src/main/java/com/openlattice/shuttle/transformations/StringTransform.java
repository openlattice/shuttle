package com.openlattice.shuttle.transformations;

import com.openlattice.client.serialization.SerializableFunction;
import com.openlattice.shuttle.EntityDefinition;
import com.openlattice.shuttle.util.Parsers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class StringTransform implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger( Parsers.class );

    public static SerializableFunction<Map<String,String>, ?> Identity( String columnName ) {
        SerializableFunction<Map<String,String>, ?> valuemapper = row -> row.get( columnName );
        return valuemapper;
    }

    public static SerializableFunction<Map<String,String>, ?> Prefixer( String columnName, String prefix ) {
        SerializableFunction<Map<String,String>, ?> valuemapper = row -> prefix+row.get( columnName );
        return valuemapper;
    }

}
