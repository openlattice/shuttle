package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ReplaceTransform extends Transformation<String> {

    private final List<String> target;
    private final Boolean ignoreCase;
    private final Boolean partial;
    private final List<String> goal;

    /**
     * Represents a transformation to replace a string by a string.
     *
     * @param target:     list of string to replace
     * @param ignoreCase: if case should be ignored
     * @param partial: if strings should be replaced when only part of the column matches the pattern
     * @param goal:       list of string to replace target by
     */
    @JsonCreator
    public ReplaceTransform(
            @JsonProperty(Constants.TARGET) List<String> target,
            @JsonProperty(Constants.IGNORE_CASE) Optional<Boolean> ignoreCase,
            @JsonProperty(Constants.PARTIAL) Optional<Boolean> partial,
            @JsonProperty(Constants.GOAL) List<String> goal
    ) {
        this.ignoreCase = ignoreCase == null ? false : true;
        this.goal = goal;
        this.partial = partial == null ? false : true;

        if (this.ignoreCase) {
            this.target = target.stream().map(value -> value.toLowerCase()).collect(Collectors.toList());
        } else {
            this.target = target;
        }

    }

    @Override
    public Object apply(String o) {
        if (StringUtils.isBlank(o)) {
            return null;
        }
        if (partial) {
            for (int i = 0; i < target.size(); ++i) {
                if (ignoreCase) {
                    o = o.replaceAll("(?i)" + target.get(i), goal.get(i));
                } else {
                    o = o.replaceAll(target.get(i), goal.get(i));
                }
            }
        } else {
            int ind = -1;
            if (ignoreCase) {
                ind = target.indexOf(o.toLowerCase());
            } else {
                ind = target.indexOf(o);
            }
            if (ind == -1) {
                return null;
            } else {
                return goal.get(ind);
            }
        }

        return o;
    }

}
