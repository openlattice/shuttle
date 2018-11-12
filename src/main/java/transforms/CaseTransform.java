package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CaseTransform extends Transformation<String> {

    private static final Logger logger = LoggerFactory
            .getLogger(Shuttle.class);

    private final String type;

    /**
     * Represents a transformation to add a prefix.
     *
     * @param type: how to case: name, lower, upper
     */
    @JsonCreator
    public CaseTransform(
            @JsonProperty(Constants.TYPE) String type
    ) {
        this.type = type == null ? "name" : type;
    }

    @Override
    public Object apply(String o) {
        if (this.type == "name") {
            return o.substring(0, 1).toUpperCase() + o.substring(1).toLowerCase();
        } else if (this.type == "lower") {
            return o.toLowerCase();
        } else if (this.type == "upper") {
            return o.toUpperCase();
        } else {
            logger.error("Unknown type of caseing: " + type);
        }
        return null;
    }

}
