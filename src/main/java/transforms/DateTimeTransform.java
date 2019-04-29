package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.dates.JavaDateTimeHelper;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;

import java.util.Optional;
import java.util.TimeZone;

public class DateTimeTransform extends Transformation<String> {
    private final String[] patterns;
    private final TimeZone timezone;

    /**
     * Represents a transformation from string to datetime.
     *
     * @param patterns:  pattern of date (eg. "MM/dd/YY")
     * @param timezone: name of the timezone
     */
    @JsonCreator
    public DateTimeTransform(
            @JsonProperty( Constants.PATTERN ) String[] patterns,
            @JsonProperty( Constants.TIMEZONE ) Optional<String> timezone ) {
        this.patterns = patterns;
        this.timezone = TimeZone.getTimeZone( timezone.orElse("America/New_York") );
    }

    public DateTimeTransform( @JsonProperty( Constants.PATTERN ) String[] patterns ) {
        this( patterns, Optional.empty() );
    }

    @JsonProperty( value = Constants.PATTERN, required = false )
    public String[] getPattern() {
        return patterns;
    }

    @JsonProperty( value = Constants.TIMEZONE, required = false )
    public TimeZone getTimezone() {
        return timezone;
    }

    @Override
    public Object applyValue( String o ) {
        final JavaDateTimeHelper dtHelper = new JavaDateTimeHelper( this.timezone, patterns );
        Object out = dtHelper.parseDateTime( o );
        return out;
    }

}
