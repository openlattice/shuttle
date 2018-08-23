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
    private final Boolean ignorecase;
    private final List<String> goal;

    /**
     * Represents a transformation to replace a string by a string.
     *
     * @param target:     list of string to replace
     * @param ignorecase: if case should be ignored
     * @param goal:       list of string to replace target by
     */
    @JsonCreator
    public ReplaceTransform(
            @JsonProperty(Constants.TARGET) List<String> target,
            @JsonProperty(Constants.IGNORE_CASE) Optional<Boolean> ignorecase,
            @JsonProperty(Constants.GOAL) List<String> goal
    ) {
        this.ignorecase = ignorecase == null ? false : true;
        this.goal = goal;

        if (this.ignorecase) {
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
        int ind = -1;
        if (ignorecase) {
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

}
