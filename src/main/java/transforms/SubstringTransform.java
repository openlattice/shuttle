package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

public class SubstringTransform extends Transformation<String> {
    private final int index;

    /**
     * Represents a transformation to get a substring *if* there is a certain prefix.
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
        return o.substring( index );
    }
}