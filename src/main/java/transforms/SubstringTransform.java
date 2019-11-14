package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;
import java.util.Objects;


public class SubstringTransform extends Transformation<String> {
    private final int index;
    private final int endIndex;

    /**
     * Represents a transformation to get a substring starting from a certain index (eg 3th character: index = 2).
     *
     * @param index: where to start subsetting if prefix is found
     * @param endIndex: where to end subsetting if prefix is found
     */
    @JsonCreator
    public SubstringTransform(
            @JsonProperty( Constants.INDEX ) Integer index,
            @JsonProperty( Constants.END_INDEX ) Integer endIndex ) {
        this.index = index;
        this.endIndex = endIndex;
    }

    @JsonProperty( Constants.INDEX )
    public int getIndex() {
        return index;
    }

    @JsonProperty( Constants.END_INDEX )
    public int getEndIndex() {
        return endIndex;
    }

    @Override
    public Object applyValue( String o ) {
        final String output;
        if ( Objects.equals( endIndex, null ) ){
            output = o.substring( index );
        } else {
            output = o.substring( index, endIndex );
        }
        return output;
    }
}
