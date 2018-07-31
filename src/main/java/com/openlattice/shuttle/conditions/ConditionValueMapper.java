package com.openlattice.shuttle.conditions;

import com.openlattice.client.serialization.SerializableFunction;

import java.util.List;
import java.util.Map;

public class ConditionValueMapper implements SerializableFunction<Map<String, String>, Object> {
    private final List<Condition> conditions;

    public ConditionValueMapper( List<Condition> conditions ) {
        this.conditions = conditions;
    }

    @Override public Object apply( Map<String, String> input ) {
        Object out = false;
        for ( Condition t : conditions ) {
            Object temp = t.apply( input );
            if (((Boolean) out).booleanValue()){
                out = true;
            }
        }
        return out;
    }
}
