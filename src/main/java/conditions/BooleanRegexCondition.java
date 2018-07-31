package conditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.openlattice.shuttle.conditions.Condition;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BooleanRegexCondition extends Condition<Map<String, String>> {
    private final String                                       column;
    private final String                                       pattern;
    private final Boolean                                      reverse;

    /**
     * Represents a transformation to select columns based on non-empty cells.
     * Function goes over columns until a non-zero input is found.
     *
     * @param column:            column to test for pattern
     * @param pattern:           pattern to test column against
     * @param reverse:           if the condition needs to be reversed (not)
     */
    @JsonCreator
    public BooleanRegexCondition(
            @JsonProperty( Constants.COLUMN ) String column,
            @JsonProperty( Constants.PATTERN ) String pattern,
            @JsonProperty( Constants.REVERSE ) Boolean reverse ) {
        this.column = column;
        this.pattern = pattern;
        this.reverse = reverse == null ? false : reverse ;

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
        if ( StringUtils.isBlank( o )) {
            return false;
        }
        Pattern p = Pattern
                .compile( this.pattern, Pattern.CASE_INSENSITIVE );
        Matcher m = p.matcher( o );

        Boolean out = false;
        if ( StringUtils.isNotBlank( o ) ) {
            if ( m.find() ) {
                out = true;
            }
        }

        if (reverse == true){
            if (out){
                out = false;
            } else {
                out = true;
            }
        }

        return out;
    }

 }

