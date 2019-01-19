package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializableFunction;
import com.openlattice.shuttle.transformations.TransformValueMapper;
import com.openlattice.shuttle.transformations.BooleanTransformation;
import com.openlattice.shuttle.transformations.Transformations;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class BooleanPrefixTransform extends BooleanTransformation {
    private final String  prefix;
    private final String  column;
    private final Boolean ignoreCase;

    /**
     * Represents a selection of transformations based on whether a column
     * contains a specific prefix or not. If either transformsIfTrue or transformsIfFalse are empty,
     * the value of the tested column will be passed on.  In principle this could be replaced
     * with BooleanRegexTransform with regex = "prefix$"
     *
     * @param column:            column to test if starts with prefix
     * @param prefix:            prefix to test value
     * @param ignoreCase:        whether to ignore case in string
     * @param transformsIfTrue:  transformations to do on column value if starts with prefix
     * @param transformsIfFalse: transformations to do if does not exist (note ! define columntransform to choose column !)
     */
    @JsonCreator
    public BooleanPrefixTransform(
            @JsonProperty( Constants.PREFIX ) String prefix,
            @JsonProperty( Constants.COLUMN ) String column,
            @JsonProperty( Constants.IGNORE_CASE ) Optional<Boolean> ignoreCase,
            @JsonProperty( Constants.TRANSFORMS_IF_TRUE ) Optional<Transformations> transformsIfTrue,
            @JsonProperty( Constants.TRANSFORMS_IF_FALSE ) Optional<Transformations> transformsIfFalse ) {
        super( transformsIfTrue, transformsIfFalse );
        this.column = column;
        this.ignoreCase = ignoreCase.orElse(false);
        if ( this.ignoreCase ) {
            this.prefix = prefix.toLowerCase();
        } else {
            this.prefix = prefix;
        }
    }

    @JsonProperty( Constants.COLUMN )
    public String getColumn() {
        return column;
    }

    @Override
    public boolean applyCondition( Map<String, String> row ) {

        if ( !( row.containsKey( column ) ) ) {
            throw new IllegalStateException( String.format( "The column %s is not found.", column ) );
        }

        final String o;
        if ( this.ignoreCase ) {
            o = row.get( column ).toLowerCase();
        } else {
            o = row.get( column );
        }
        if ( StringUtils.isNotBlank( o ) ) {
            if ( o.startsWith( prefix ) ) {
                return true;
            }
        }
        return false;
    }
}

