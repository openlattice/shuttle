package com.openlattice.shuttle.transformations;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.openlattice.client.serialization.SerializableFunction;
import com.openlattice.shuttle.EntityDefinition;
import com.openlattice.shuttle.util.Parsers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

@JsonTypeInfo(use=JsonTypeInfo.Id.CLASS, include= JsonTypeInfo.As.PROPERTY, property="@transform")
public class StringTransform implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger( Parsers.class );

    public static SerializableFunction<Map<String,String>, ?> Prefixer( String columnName, String prefix ) {
        SerializableFunction<Map<String,String>, ?> valuemapper = row -> prefix+row.get(columnName);
        return valuemapper;
    }

}
