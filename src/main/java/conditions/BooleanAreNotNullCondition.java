package conditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.conditions.Condition;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;
import transforms.CaseTransform;
import transforms.ConcatTransform;

import java.util.List;
import java.util.Map;

public class BooleanAreNotNullCondition extends Condition<Map<String, String>> {
    private final List<String> columns;

    public enum CombinationType {all, any, none};
    private final BooleanAreNotNullCondition.CombinationType type;
    private final Boolean reverse;

    /**
     * Represents a condition to select columns based on non-empty cells (multiple columns).
     *
     * @param columns:  column to test for pattern
     * @param type: the kind of combination for multiple booleans: any, all or none
     * @param reverse: if the condition needs to be reversed (i.e. true if pattern *not* present)
     */
    @JsonCreator
    public BooleanAreNotNullCondition(
            @JsonProperty( Constants.COLUMNS ) List<String> columns,
            @JsonProperty( Constants.TYPE ) BooleanAreNotNullCondition.CombinationType type,
            @JsonProperty( Constants.REVERSE ) Boolean reverse) {
        this.columns = columns;
        this.type = type == null ? CombinationType.all : type;
        this.reverse = reverse == null ? false : reverse;

    }

    @JsonProperty( Constants.COLUMNS )
    public List<String> getColumn() {
        return columns;
    }

    @JsonProperty( Constants.REVERSE )
    public Boolean getReverse() {
        return reverse;
    }

    @Override
    public Boolean apply( Map<String, String> row ) {
        Integer count = 0;
        for ( String s : columns ) {
            if ( !( row.containsKey( s ) ) ) {
                throw new IllegalStateException( String.format( "The column %s is not found.", s ) );
            }
            if ( !StringUtils.isBlank( row.get( s ) ) ) {
                count = count + 1;

            }
        }

        Boolean out = false;
        switch ( type ) {
            case all:
                out = count == columns.size();
                break;
            case any:
                out = count > 0;
                break;
            case none:
                out = count == 0;
                break;
            default:
                out = false;
                break;
        }

        if ( reverse == true ) {
            out = !out;
        }

        return out;

    }

}

