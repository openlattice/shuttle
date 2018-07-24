package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

import static com.openlattice.shuttle.transformations.Transformation.TRANSFORM;

@JsonIgnoreProperties( value = { TRANSFORM } )
public class ConditionalPrefixSubstringTransform extends Transformation<String> {
    private final String prefix;
    private final int index;

    /**
     * Represents a transformation to get a substring *if* there is a certain prefix.
     * @param prefix: string to present conditional
     * @param index: where to start subsetting if prefix is found
     */
    @JsonCreator
    public ConditionalPrefixSubstringTransform(
            @JsonProperty( Constants.PREFIX ) String prefix,
            @JsonProperty( Constants.INDEX ) int index ) {
        this.prefix = prefix;
        this.index = index;
    }

    @JsonProperty( Constants.PREFIX )
    public String getPrefix() {
        return prefix;
    }

    @JsonProperty( Constants.LOC )
    public int getSubstringLocation() {
        return index;
    }

    @Override
    public Object apply( String o ) {
        if ( StringUtils.isBlank(o)) {return ""; }
        if ( o.startsWith(prefix) ) {
            return o.substring( index );
        }
        return o;
    }
}
