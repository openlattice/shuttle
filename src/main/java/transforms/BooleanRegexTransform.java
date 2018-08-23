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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BooleanRegexTransform extends Transformation<Map<String, String>> {
    private final String column;
    private final SerializableFunction<Map<String, String>, ?> truevalueMapper;
    private final SerializableFunction<Map<String, String>, ?> falsevalueMapper;
    private final Optional<Transformations> transformsiftrue;
    private final Optional<Transformations> transformsiffalse;
    private final String pattern;

    /**
     * Represents a selection of transformations based on whether a column
     * contains a specific regular expression or not.  If either transformsiftrue or transformsiffalse are empty,
     * the value of the tested column will be passed on.
     *
     * @param column:            column to test if column contains regex.
     * @param pattern:           regex to test column against
     * @param transformsiftrue:  transformations to do on column value if pattern is present
     * @param transformsiffalse: transformations to do if does not exist (note ! define columntransform to choose column !)
     */
    @JsonCreator
    public BooleanRegexTransform(
            @JsonProperty(Constants.COLUMN) String column,
            @JsonProperty(Constants.TRANSFORMS_IF_TRUE) Optional<Transformations> transformsiftrue,
            @JsonProperty(Constants.PATTERN) String pattern,
            @JsonProperty(Constants.TRANSFORMS_IF_FALSE) Optional<Transformations> transformsiffalse) {
        this.column = column;
        this.pattern = pattern;
        this.transformsiftrue = transformsiftrue;
        this.transformsiffalse = transformsiffalse;

        // true valuemapper
        if (transformsiftrue.isPresent()) {
            final List<Transformation> internalTrueTransforms;
            internalTrueTransforms = new ArrayList<>(this.transformsiftrue.get().size() + 1);
            transformsiftrue.get().forEach(internalTrueTransforms::add);
            this.truevalueMapper = new TransformValueMapper(internalTrueTransforms);
        } else {
            this.truevalueMapper = row -> row.get(column);
        }

        // false valuemapper
        if (transformsiffalse.isPresent()) {
            final List<Transformation> internalFalseTransforms;
            internalFalseTransforms = new ArrayList<>(this.transformsiffalse.get().size() + 1);
            transformsiffalse.get().forEach(internalFalseTransforms::add);
            this.falsevalueMapper = new TransformValueMapper(internalFalseTransforms);
        } else {
            this.falsevalueMapper = row -> row.get(column);
        }
    }

    @JsonProperty(Constants.TRANSFORMS_IF_TRUE)
    public Optional<Transformations> getTransformsIfTrue() {
        return transformsiftrue;
    }

    @JsonProperty(Constants.TRANSFORMS_IF_FALSE)
    public Optional<Transformations> getTransformsIfFalse() {
        return transformsiffalse;
    }

    @JsonProperty(Constants.COLUMN)
    public String getColumn() {
        return column;
    }

    @JsonProperty(Constants.PATTERN)
    public String getPattern() {
        return pattern;
    }

    @Override
    public Object apply(Map<String, String> row) {
        String o = row.get(column);
        if (StringUtils.isBlank(o)) {
            return this.falsevalueMapper.apply(row);
        }
        Pattern p = Pattern
                .compile(this.pattern, Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(o);

        if (m.find()) {
            return this.truevalueMapper.apply(row);
        } else {
            return this.falsevalueMapper.apply(row);
        }

    }

}

