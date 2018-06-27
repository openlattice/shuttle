package com.openlattice.shuttle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.openlattice.client.serialization.SerializableFunction;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.shuttle.adapter.Row;
import com.openlattice.shuttle.transformations.Transform;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import com.openlattice.shuttle.util.Constants;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class TransformationDefinition implements Serializable {

    private static final long serialVersionUID = -6632902808080642647L;

    private String                           prefix;
    private String                           transformation;

    private final Transform transformer = new Transform();

    @JsonCreator
    public TransformationDefinition(
            @JsonProperty( Constants.TRANSFORMATION ) String transformation,
            @JsonProperty( Constants.PREFIX ) String prefix ) {
        this.transformation = transformation;
        this.prefix = prefix;
    }

    @JsonProperty( Constants.TRANSFORMATION )
    public String getFunction() {return this.transformation; }

    @JsonProperty( Constants.PREFIX )
    public String getPrefix() {return this.prefix;}

}
