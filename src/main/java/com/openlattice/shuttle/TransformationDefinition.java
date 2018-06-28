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
import java.util.Optional;

public class TransformationDefinition implements Serializable {

    private static final long serialVersionUID = -6632902808080642647L;

    private String prefix;

    private final Transform transformer = new Transform();

    // THIS SHOULD BE A LIST OF FUNCTIONS
    @JsonCreator
    public TransformationDefinition(
            @JsonProperty( value = Constants.PREFIX ) String prefix ) {
        this.prefix = prefix;
    }

    @JsonProperty( Constants.PREFIX )
    public String getPrefix() {
        return this.prefix;
    }

}