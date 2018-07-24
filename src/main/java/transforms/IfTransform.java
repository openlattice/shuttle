package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

public class IfTransform extends Transformation<String> {

    private final String pattern;
    private final String value;
    private final String valueelse;

    /**
     * Represents a transformation to replace a string by a string.
     * Unfortunately we can only have 1 stream at the moment.
     * @param pattern: string to look for
     * @param value: string to return if column contains pattern,
     *             if value is not specified: return column value
     * @param valueelse: string to return if column does not contain pattern,
     *             if valueelse is not specified: return column value
     */
    @JsonCreator
    public IfTransform(
            @JsonProperty( Constants.PATTERN ) String pattern ,
            @JsonProperty (Constants.VALUE ) String value,
            @JsonProperty (Constants.ELSE ) String valueelse
    ) {
        this.pattern = pattern;
        this.value = value;
        this.valueelse = valueelse;
    }

    @Override public Object apply( String o ) {
        if ( o.contains( pattern ) ) {
            if ( value == null ) {
                return o;
            } else {
                return value;
            }
        } else {
            if ( valueelse == null ) {
                return o;
            } else {
                return valueelse;
            }
        }
    }
}
