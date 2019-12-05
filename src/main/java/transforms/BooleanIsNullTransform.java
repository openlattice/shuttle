package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.BooleanTransformation;
import com.openlattice.shuttle.transformations.Transformations;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Optional;

public class BooleanIsNullTransform<I extends Object> extends BooleanTransformation<I> {
    private final String column;

    /**
     * Represents a selection of transformations based on empty cells.  If either transformsiftrue or transformsiffalse are empty,
     * the value of the tested column will be passed on.
     *
     * @param column:            column to test if is null (note: true = cell is empty)
     * @param transformsIfTrue:  transformations to do on column value if exists
     * @param transformsIfFalse: transformations to do if does not exist (note ! define columntransform to choose column !)
     */
    @JsonCreator
    public BooleanIsNullTransform(
            @JsonProperty( Constants.COLUMN ) String column,
            @JsonProperty( Constants.TRANSFORMS_IF_TRUE ) Optional<Transformations> transformsIfTrue,
            @JsonProperty( Constants.TRANSFORMS_IF_FALSE ) Optional<Transformations> transformsIfFalse ) {
        super( transformsIfTrue, transformsIfFalse );
        this.column = column;

    }

    @JsonProperty( Constants.COLUMN )
    public String getColumn() {
        return column;
    }

    @Override
    public boolean applyCondition( Map<String, Object> row ) {
        if ( !( row.containsKey( column ) ) ) {
            throw new IllegalStateException( String.format( "The column %s is not found.", column ) );
        }

        if ( row.get( column ) == null ) {
            return true;
        }

        if ( row.get( column ) instanceof String ) {
            return StringUtils.isEmpty( (String) row.get( column ) );
        }

        return false;
    }
}

