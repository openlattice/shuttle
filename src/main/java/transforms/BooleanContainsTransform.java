package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializableFunction;
import com.openlattice.shuttle.transformations.TransformValueMapper;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.transformations.Transformations;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class BooleanContainsTransform extends Transformation<Map<String, String>> {
    private final String column;
    private final String pattern;
    private final SerializableFunction<Map<String, String>, ?> trueValueMapper;
    private final SerializableFunction<Map<String, String>, ?> falseValueMapper;
    private final Optional<Transformations> transformsIfTrue;
    private final Optional<Transformations> transformsIfFalse;

    /**
     * Represents a selection of transformations based on whether a column
     * contains a specific value or not.  If either transformsIfTrue or transformsIfFalse are empty,
     * the value of the tested column will be passed on.
     *
     * @param column:            column to test for pattern
     * @param pattern:           pattern to test column against
     * @param transformsIfTrue:  transformations to do on column value if exists
     * @param transformsIfFalse: transformations to do if does not exist (note ! define columntransform to choose column !)
     */
    @JsonCreator
    public BooleanContainsTransform(
            @JsonProperty(Constants.COLUMN) String column,
            @JsonProperty(Constants.PATTERN) String pattern,
            @JsonProperty(Constants.TRANSFORMS_IF_TRUE) Optional<Transformations> transformsIfTrue,
            @JsonProperty(Constants.TRANSFORMS_IF_FALSE) Optional<Transformations> transformsIfFalse) {
        this.column = column;
        this.pattern = pattern;
        this.transformsIfTrue = transformsIfTrue;
        this.transformsIfFalse = transformsIfFalse;

        // true valuemapper
        if (transformsIfTrue.isPresent()) {
            final List<Transformation> internalTrueTransforms;
            internalTrueTransforms = new ArrayList<>(this.transformsIfTrue.get().size() + 1);
            transformsIfTrue.get().forEach(internalTrueTransforms::add);
            this.trueValueMapper = new TransformValueMapper(internalTrueTransforms);
        } else {
            this.trueValueMapper = row -> row.get(column);
        }

        // false valuemapper
        if (transformsIfFalse.isPresent()) {
            final List<Transformation> internalFalseTransforms;
            internalFalseTransforms = new ArrayList<>(this.transformsIfFalse.get().size() + 1);
            transformsIfFalse.get().forEach(internalFalseTransforms::add);
            this.falseValueMapper = new TransformValueMapper(internalFalseTransforms);
        } else {
            this.falseValueMapper = row -> row.get(column);
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

    @JsonProperty(Constants.COLUMN)
    public String getColumn() {
        return column;
    }

    @Override
    public Object apply(Map<String, String> row) {
        String o = row.get(column);
        if (StringUtils.isNotBlank(o)) {
            if (o.contains(pattern)) {
                return this.trueValueMapper.apply(row);
            }
        }
        return this.falseValueMapper.apply(row);
    }
}

