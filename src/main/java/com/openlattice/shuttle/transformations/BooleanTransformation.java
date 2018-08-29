package com.openlattice.shuttle.transformations;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializableFunction;
import com.openlattice.shuttle.util.Constants;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class BooleanTransformation extends Transformation<Map<String, String>> {
    private final SerializableFunction<Map<String, String>, ?> trueValueMapper;
    private final SerializableFunction<Map<String, String>, ?> falseValueMapper;
    private final Optional<Transformations> transformsIfTrue;
    private final Optional<Transformations> transformsIfFalse;

    /**
     * Represents a selection of transformations based on empty cells.  If either transformsiftrue or transformsiffalse are empty,
     * the value of the tested column will be passed on.
     *
     * @param transformsIfTrue:  transformations to do on column value if exists
     * @param transformsIfFalse: transformations to do if does not exist (note ! define columntransform to choose column !)
     */
    @JsonCreator
    public BooleanTransformation(
            @JsonProperty(Constants.TRANSFORMS_IF_TRUE) Optional<Transformations> transformsIfTrue,
            @JsonProperty(Constants.TRANSFORMS_IF_FALSE) Optional<Transformations> transformsIfFalse) {
        this.transformsIfTrue = transformsIfTrue;
        this.transformsIfFalse = transformsIfFalse;

        // true valuemapper
        if (transformsIfTrue.isPresent()) {
            final List<Transformation> internalTrueTransforms;
            internalTrueTransforms = new ArrayList<>(this.transformsIfTrue.get().size());
            transformsIfTrue.get().forEach(internalTrueTransforms::add);
            this.trueValueMapper = new TransformValueMapper(internalTrueTransforms);
        } else {
            this.trueValueMapper = row -> null;
        }

        // false valuemapper
        if (transformsIfFalse.isPresent()) {
            final List<Transformation> internalFalseTransforms;
            internalFalseTransforms = new ArrayList<>(this.transformsIfFalse.get().size());
            transformsIfFalse.get().forEach(internalFalseTransforms::add);
            this.falseValueMapper = new TransformValueMapper(internalFalseTransforms);
        } else {
            this.falseValueMapper = row -> null;
        }
    }

    @JsonProperty(Constants.TRANSFORMS_IF_TRUE)
    public Optional<Transformations> getTransformsIfTrue() {
        return transformsIfTrue;
    }

    @JsonProperty(Constants.TRANSFORMS_IF_FALSE)
    public Optional<Transformations> getTransformsIfFalse() {
        return transformsIfFalse;
    }

    public SerializableFunction<Map<String, String>, ?> getTrueValueMapper() {
        return trueValueMapper;
    }

    public SerializableFunction<Map<String, String>, ?> getFalseValueMapper() {
        return falseValueMapper;
    }

    public boolean applyCondition(Map<String, String> row ) {
        return true;
    }

    public Object apply(Map<String, String> row) {
        return applyCondition(row) ? this.trueValueMapper.apply(row) : this.falseValueMapper.apply(row);
    }
}

