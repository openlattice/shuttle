package conditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.conditions.Condition;
import com.openlattice.shuttle.util.Cached;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.regex.Matcher;

public class BooleanRegexCondition extends Condition<Map<String, String>> {
    private final String  column;
    private final String  pattern;
    private final Boolean reverse;

    /**
     * Represents a condition to select columns based on a regular expression.
     *
     * @param column:  column to test for pattern
     * @param pattern: regular expression to test column against
     * @param reverse: if the condition needs to be reversed (i.e. true if pattern *not* present)
     */
    @JsonCreator
    public BooleanRegexCondition(
            @JsonProperty( Constants.COLUMN ) String column,
            @JsonProperty( Constants.PATTERN ) String pattern,
            @JsonProperty( Constants.REVERSE ) Boolean reverse ) {
        this.column = column;
        this.pattern = pattern;
        this.reverse = reverse == null ? false : reverse;

    }

    @JsonProperty( Constants.COLUMN )
    public String getColumn() {
        return column;
    }

    @JsonProperty( Constants.PATTERN )
    public String getPattern() {
        return pattern;
    }

    @JsonProperty( Constants.REVERSE )
    public Boolean getReverse() {
        return reverse;
    }

    @Override
    public Boolean apply( Map<String, String> row ) {
        String o = row.get( column );

        Matcher m = Cached.getInsensitiveMatcherForString( o, this.pattern );

        Boolean out = false;
        if ( m.find() ) {
            out = true;
        }

        if ( reverse ) {
            out = !out;
        }

        return out;
    }

}

