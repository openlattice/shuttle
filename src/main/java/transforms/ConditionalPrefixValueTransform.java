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
public class ConditionalPrefixValueTransform extends Transformation<String> {
    private final List<String> prefix;
    private final String valueelse;
    private final List<String> value;

    /**
     * Represents a transformation to get a value *if* there is a certain prefix.
     * @param prefix: string to present conditional
     * @param value: which value to set if prefix is found
     * @param valueelse: which value to set if prefix is *not* found
     */
    @JsonCreator
    public ConditionalPrefixValueTransform(
            @JsonProperty( Constants.PREFIX ) List<String> prefix,
            @JsonProperty( Constants.ELSE ) String valueelse,
            @JsonProperty( Constants.VALUE ) List<String> value ) {
        this.prefix = prefix;
        this.valueelse = valueelse;
        this.value = value;
    }

    @JsonProperty( Constants.PREFIX )
    public List<String> getPrefix() {
        return prefix;
    }

    @JsonProperty( Constants.VALUE )
    public List<String> getValue() {
        return value;
    }

    @Override
    public Object apply( String o ) {
        if ( StringUtils.isBlank(o)) {return ""; }
        for ( int i = 0; i<prefix.size(); ++i ) {
            if ( o.startsWith(prefix.get(i))) {
                return value.get(i);
            }
        }
        return valueelse;
    }
}
