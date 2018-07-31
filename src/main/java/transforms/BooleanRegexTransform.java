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
    private final String                                       column;
    private final SerializableFunction<Map<String, String>, ?> truevalueMapper;
    private final SerializableFunction<Map<String, String>, ?> falsevalueMapper;
    private final Optional<Transformations>                    transformsiftrue;
    private final Optional<Transformations>                    transformsiffalse;
    private final String                                       pattern;

    /**
     * Represents a transformation to select columns based on non-empty cells.
     * Function goes over columns until a non-zero input is found.
     *
     * @param column:            column to test if is null
     * @param pattern:           pattern to test column against
     * @param transformsiftrue:  declare transformations to do on column value if exists
     * @param transformsiffalse: declare transformations to do if does not exist (note ! define columntransform to choose column !)
     */
    @JsonCreator
    public BooleanRegexTransform(
            @JsonProperty( Constants.COLUMN ) String column,
            @JsonProperty( Constants.TRANSFORMSIFTRUE ) Optional<Transformations> transformsiftrue,
            @JsonProperty( Constants.PATTERN ) String pattern,
            @JsonProperty( Constants.TRANSFORMSIFFALSE ) Optional<Transformations> transformsiffalse ) {
        this.column = column;
        this.pattern = pattern;
        this.transformsiftrue = transformsiftrue;
        this.transformsiffalse = transformsiffalse;

        // true valuemapper
        if ( transformsiftrue.isPresent() ) {
            final List<Transformation> internalTrueTransforms;
            internalTrueTransforms = new ArrayList<>( this.transformsiftrue.get().size() + 1 );
            transformsiftrue.get().forEach( internalTrueTransforms::add );
            this.truevalueMapper = new TransformValueMapper( internalTrueTransforms );
        } else {
            this.truevalueMapper = row -> row.get( column );
        }

        // false valuemapper
        if ( transformsiffalse.isPresent() ) {
            final List<Transformation> internalFalseTransforms;
            internalFalseTransforms = new ArrayList<>( this.transformsiffalse.get().size() + 1 );
            transformsiffalse.get().forEach( internalFalseTransforms::add );
            this.falsevalueMapper = new TransformValueMapper( internalFalseTransforms );
        } else {
            this.falsevalueMapper = row -> row.get( column );
        }
    }

    @JsonProperty( Constants.TRANSFORMSIFTRUE )
    public Optional<Transformations> getTransformsIfTrue() {
        return transformsiftrue;
    }

    @JsonProperty( Constants.TRANSFORMSIFFALSE )
    public Optional<Transformations> getTransformsIfFalse() {
        return transformsiffalse;
    }

    @JsonProperty( Constants.COLUMN )
    public String getColumn() {
        return column;
    }

    @JsonProperty( Constants.PATTERN )
    public String getPattern() {
        return pattern;
    }

    @Override
    public Object apply( Map<String, String> row ) {
        String o = row.get( column );
        if ( StringUtils.isBlank( o ) ) {
            return false;
        }
        Pattern p = Pattern
                .compile( this.pattern, Pattern.CASE_INSENSITIVE );
        Matcher m = p.matcher( o );

        if ( m.find() ) {
            return this.truevalueMapper.apply( row );
        } else {
            return this.falsevalueMapper.apply( row );
        }

    }

}

