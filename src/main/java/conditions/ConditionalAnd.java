package conditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.openlattice.shuttle.conditions.Condition;

import java.util.Map;

public class ConditionalAnd extends Condition<Map<String, String>> {

    /**
     * Represents a conditional and.
     * If used: all next conditions (and previous outcome) need to be true.
     */
    @JsonCreator
    public ConditionalAnd() {
    }

    @Override
    public Boolean apply(Map<String, String> row) {
        return false;
    }

}

