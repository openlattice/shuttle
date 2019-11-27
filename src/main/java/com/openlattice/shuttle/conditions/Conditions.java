package com.openlattice.shuttle.conditions;

import com.google.common.collect.Lists;

import java.util.ArrayList;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class Conditions extends ArrayList<Condition> {
    private Conditions() {
        super();
    }

    public static Conditions of( Condition... conditions ) {
        return (Conditions) Lists.newArrayList(conditions);
    }
}
