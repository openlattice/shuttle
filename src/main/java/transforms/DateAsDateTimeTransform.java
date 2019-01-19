package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.dates.JavaDateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

public class DateAsDateTimeTransform extends Transformation<String> {
    private final String[] pattern;

    /**
     * Represents a transformation from string to datetime.
     *
     * @param pattern: pattern of date (eg. "MM/dd/YY")
     */
    @JsonCreator
    public DateAsDateTimeTransform( @JsonProperty( Constants.PATTERN ) String[] pattern ) {
        this.pattern = pattern;
    }

    @JsonProperty( value = Constants.PATTERN, required = false )
    public String[] getPattern() {
        return pattern;
    }

    @Override
    public Object applyValue( String o ) {
        if ( StringUtils.isBlank( o ) | o == null ) {
            return null;
        }
        final JavaDateTimeHelper dtHelper = new JavaDateTimeHelper( TimeZones.America_NewYork,
                pattern );
        Object out = dtHelper.parseDateAsDateTime( o );
        return out;
    }

}
