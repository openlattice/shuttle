package com.openlattice.shuttle.transformations;

import com.openlattice.client.serialization.SerializableFunction;
import com.openlattice.shuttle.TransformationDefinition;
import com.openlattice.shuttle.transformations.StringTransform;
import com.openlattice.shuttle.util.Parsers;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Transform implements Serializable {

     private static final Logger logger = LoggerFactory.getLogger( Parsers.class );

    LinkedHashMap<String, SerializableFunction<Map<String,String>, ?>> transformations =
        new LinkedHashMap<String, SerializableFunction<Map<String,String>, ?>>();

    public SerializableFunction<Map<String,String>, ?> getTransform( String columnName, TransformationDefinition transformation ) {
        transformations.put("identity", StringTransform.Identity( columnName ));
        transformations.put("prefix", StringTransform.Prefixer( columnName, transformation.getPrefix() ));

        // NEXXXTTTTTT
        // make getters for transformations and put them in here :-)
        // Look at how json stuff can be optional (or can it?)
        return transformations.get(transformation.getFunction());
    }

}
