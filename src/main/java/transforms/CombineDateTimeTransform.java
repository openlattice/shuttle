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
    private final String datecolumn;
    private final String[] datepattern;
    private final String timecolumn;
    private final String[] timepattern;


    /**
     * Represents a transformation from string to datetime.
     *
     * @param datecolumn: column of date
     * @param datepattern: list of patterns of date
     * @param timecolumn: column of time
     * @param timepattern: list of patterns of time
     *
     */
    @JsonCreator
    public CombineDateTimeTransform(
            @JsonProperty(Constants.DATE_COLUMN) String datecolumn,
            @JsonProperty(Constants.DATE_PATTERN) String[] datepattern,
            @JsonProperty(Constants.TIME_COLUMN) String timecolumn,
            @JsonProperty(Constants.TIME_PATTERN) String[] timepattern
            ) {
        this.datecolumn = datecolumn;
        this.datepattern = datepattern;
        this.timecolumn = timecolumn;
        this.timepattern = timepattern;
    }

    @Override
    public Object apply(Map<String, String> row) {

        // get date
        String d = row.get(datecolumn);
        if (StringUtils.isBlank(d) | d==null) {
            return null;
        }
        final JavaDateTimeHelper dHelper = new JavaDateTimeHelper(TimeZones.America_NewYork,
                datepattern);
        LocalDate date = dHelper.parseDate(d);

        // get time
        String t = row.get(timecolumn);
        if (StringUtils.isBlank(t) | t==null) {
            return null;
        }
        final JavaDateTimeHelper tHelper = new JavaDateTimeHelper(TimeZones.America_NewYork,
                timepattern);
        LocalTime time = tHelper.parseTime(t);

        // combine
        LocalDateTime dateTime = LocalDateTime.of(date, time);
        TimeZone tz = TimeZones.America_NewYork;
        OffsetDateTime out = dateTime.atZone( tz.toZoneId() ).toOffsetDateTime();
        return out;
    }

}
