package conditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.conditions.Condition;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;

import java.util.List;
import java.util.Map;

public class CompareCondition extends Condition<Map<String, String>> {
    private final String leftColumn, rightColumn;
    private final List<Transformation> leftTransforms, rightTransforms;

    public enum Comparison {eq, ne, ge, gt, lt, le}

    private final Comparison comparison;

    /**
     * Represents a condition to select columns based on a regular expression.
     *
     * @param leftColumn:      the column whose value will be on the left side of the comparison
     * @param leftTransforms:  transformations to apply to the left column
     * @param rightColumn:     the column whose value will be on the right side of the comparison
     * @param rightTransforms: transformations to apply to the right column
     * @param comparison:      direction of comparison : {eq, ne, ge, gt, lt, le}
     */
    @JsonCreator
    public CompareCondition(
            @JsonProperty( Constants.LEFTCOLUMN ) String leftColumn,
            @JsonProperty( Constants.LEFTTRANSFORMS ) List<Transformation> leftTransforms,
            @JsonProperty( Constants.RIGHTCOLUMN ) String rightColumn,
            @JsonProperty( Constants.RIGHTTRANSFORMS ) List<Transformation> rightTransforms,
            @JsonProperty( Constants.COMPARISON ) Comparison comparison ) {
        this.leftColumn = leftColumn;
        this.leftTransforms = leftTransforms;
        this.rightColumn = rightColumn;
        this.rightTransforms = rightTransforms;
        this.comparison = comparison == null ? Comparison.le : comparison;
    }

    @JsonProperty( Constants.LEFTCOLUMN )
    public String getLeftColumn() {
        return leftColumn;
    }

    @JsonProperty( Constants.LEFTTRANSFORMS )
    public List<Transformation> getLeftTransforms() {
        return leftTransforms;
    }

    @JsonProperty( Constants.RIGHTCOLUMN )
    public String getRightColumn() {
        return rightColumn;
    }

    @JsonProperty( Constants.RIGHTTRANSFORMS )
    public List<Transformation> getRightTransforms() {
        return rightTransforms;
    }

    @JsonProperty( Constants.COMPARISON )
    public Comparison getComparison() {
        return comparison;
    }

    @Override
    public Boolean apply( Map<String, String> row ) {
        // return <transformed leftColumn> <comparison> <transformed rightColumn>

        Object leftTransformed = row.get( leftColumn );
        Object rightTransformed = row.get( rightColumn );

        if ( !(leftColumn == null) && !( row.containsKey( leftColumn ) ) ) {
            throw new IllegalStateException( String.format( "The column %s is not found.", leftColumn ) );
        }
        if ( !(rightColumn == null) && !( row.containsKey( rightColumn ) ) ) {
            throw new IllegalStateException( String.format( "The column %s is not found.", rightColumn ) );
        }

        if ( (!(leftColumn == null) &&  leftTransformed == null) ||  (!(rightColumn == null) &&  rightTransformed == null) ) {
            return Boolean.FALSE;
        }

        for ( Transformation t : leftTransforms ) {
            leftTransformed = t.apply( leftTransformed );
        }
        for ( Transformation t : rightTransforms ) {
            rightTransformed = t.apply( rightTransformed );
        }

        if ( leftTransformed == null || rightTransformed == null ) {
            return Boolean.FALSE;
        }

        try {
            switch ( comparison ) {

                case eq:
                    return ( (Comparable) leftTransformed ).compareTo( rightTransformed ) == 0;
                case ne:
                    return ( (Comparable) leftTransformed ).compareTo( rightTransformed ) != 0;
                case ge:
                    return ( (Comparable) leftTransformed ).compareTo( rightTransformed ) >= 0;
                case gt:
                    return ( (Comparable) leftTransformed ).compareTo( rightTransformed ) > 0;
                case lt:
                    return ( (Comparable) leftTransformed ).compareTo( rightTransformed ) < 0;
                case le:
                    return ( (Comparable) leftTransformed ).compareTo( rightTransformed ) <= 0;
                default:
                    return false;
            }
        } catch ( ClassCastException e ) {
            return null;
        }
    }
}

