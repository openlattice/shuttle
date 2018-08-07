package conditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.openlattice.shuttle.conditions.Condition;

import java.util.Map;

public class ConditionalOr extends Condition<Map<String, String>> {

    /**
     * Represents a conditional or.
     * If used: one of the next conditions (or previous outcome) need to be true.
     */
    @JsonCreator
    public ConditionalOr() {
    }

    @Override
    public Boolean apply(Map<String, String> row) {
        return false;
    }

}

