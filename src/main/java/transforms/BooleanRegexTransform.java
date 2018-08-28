package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializableFunction;
import com.openlattice.shuttle.transformations.TransformValueMapper;
import com.openlattice.shuttle.transformations.BooleanTransformation;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.transformations.Transformations;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BooleanRegexTransform extends BooleanTransformation {
    private final String column;
    private final String pattern;

    /**
     * Represents a selection of transformations based on whether a column
     * contains a specific regular expression or not.  If either transformsIfTrue or transformsIfFalse are empty,
     * the value of the tested column will be passed on.
     *
     * @param column:            column to test if column contains regex.
     * @param pattern:           regex to test column against
     */
    @JsonCreator
    public BooleanRegexTransform(
            @JsonProperty(Constants.COLUMN) String column,
            @JsonProperty(Constants.PATTERN) String pattern) {
        this.column = column;
        this.pattern = pattern;


    @Override
    public Object applyCondition(Map<String, String> row) {
        String o = row.get(column);
        if (StringUtils.isBlank(o)) {
            return this.falseValueMapper.apply(row);
        }
        Pattern p = Pattern
                .compile(this.pattern, Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(o);

        if (m.find()) {
            return this.trueValueMapper.apply(row);
        } else {
            return this.falseValueMapper.apply(row);
        }

    }

}

