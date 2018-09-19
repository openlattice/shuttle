package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.dates.JavaDateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.TimeZone;

public class CombineDateTimeTransform extends Transformation<Map<String, String>> {
    private final String   dateColumn;
    private final String[] datePattern;
    private final String   timeColumn;
    private final String[] timePattern;

    /**
     * Represents a transformation from string to datetime.
     *
     * @param dateColumn:  column of date
     * @param datePattern: list of patterns of date
     * @param timeColumn:  column of time
     * @param timePattern: list of patterns of time
     */
    @JsonCreator
    public CombineDateTimeTransform(
            @JsonProperty( Constants.DATE_COLUMN ) String dateColumn,
            @JsonProperty( Constants.DATE_PATTERN ) String[] datePattern,
            @JsonProperty( Constants.TIME_COLUMN ) String timeColumn,
            @JsonProperty( Constants.TIME_PATTERN ) String[] timePattern
    ) {
        this.dateColumn = dateColumn;
        this.datePattern = datePattern;
        this.timeColumn = timeColumn;
        this.timePattern = timePattern;
    }

    @Override
    public Object apply( Map<String, String> row ) {

        // get date
        String d = row.get( dateColumn );
        if ( StringUtils.isBlank( d ) | d == null ) {
            return null;
        }
        final JavaDateTimeHelper dHelper = new JavaDateTimeHelper( TimeZones.America_NewYork,
                datePattern );
        LocalDate date = dHelper.parseDate( d );

        // get time
        String t = row.get( timeColumn );
        if ( StringUtils.isBlank( t ) | t == null ) {
            return null;
        }
        final JavaDateTimeHelper tHelper = new JavaDateTimeHelper( TimeZones.America_NewYork,
                timePattern );
        LocalTime time = tHelper.parseTime( t );

        // combine
        LocalDateTime dateTime = LocalDateTime.of( date, time );
        TimeZone tz = TimeZones.America_NewYork;
        OffsetDateTime out = dateTime.atZone( tz.toZoneId() ).toOffsetDateTime();
        return out;
    }

}
