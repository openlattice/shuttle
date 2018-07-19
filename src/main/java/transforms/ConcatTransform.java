package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;

import java.util.List;
import java.util.Map;

import static com.openlattice.shuttle.transformations.Transformation.TRANSFORM;

@JsonIgnoreProperties( value = { TRANSFORM } )
public class ConcatTransform extends Transformation<Map<String, String>> {
    private final List<String> columns;
    private final String       separator;

    /**
     * Represents a transformation to concatenate values.
     * Function selects all columns and concatenates them with the separator
     * @param columns: list of column names to go over in sequential order
     * @param separator: separator to concatenate the values
     */
    @JsonCreator
    public ConcatTransform(
            @JsonProperty( Constants.COLUMNS ) List<String> columns,
            @JsonProperty( Constants.SEP ) String separator ) {
        this.columns = columns;
        this.separator = separator;
    }

    @JsonProperty( Constants.SEP )
    public String getSeparator() {
        return separator;
    }

    @JsonProperty( Constants.COLUMNS )
    public List<String> getColumns() {
        return columns;
    }

    @Override public Object apply( Map<String, String> row ) {
        StringBuilder sb = new StringBuilder();
        String sep = "";
        for ( String s : columns ) {
            sb.append( sep ).append( row.get( s ) );
            sep = separator;
        }
        return sb.toString();
    }

}

