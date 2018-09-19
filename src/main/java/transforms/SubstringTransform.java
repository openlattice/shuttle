package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

public class SubstringTransform extends Transformation<String> {
    private final int index;

    /**
     * Represents a transformation to get a substring starting from a certain index (eg 3th character: index = 2).
     *
     * @param index: where to start subsetting if prefix is found
     */
    @JsonCreator
    public SubstringTransform(
            @JsonProperty( Constants.INDEX ) int index ) {
        this.index = index;
    }

    @JsonProperty( Constants.LOC )
    public int getSubstringLocation() {
        return index;
    }

    @Override
    public Object apply( String o ) {
        final String output;
        if ( StringUtils.isBlank( o ) ) {
            return null;
        } else {
            output = o.substring( index );
        }
        if ( !StringUtils.isBlank( output ) ) {
            return null;
        }
        return output;
    }
}
