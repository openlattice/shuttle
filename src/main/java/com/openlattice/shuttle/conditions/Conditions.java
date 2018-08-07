package com.openlattice.shuttle.conditions;

import com.openlattice.shuttle.transformations.Transformation;

import java.util.ArrayList;
import java.util.Collection;
import org.jetbrains.annotations.NotNull;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class Conditions extends ArrayList<Condition> {
    public Conditions( int initialCapacity ) {
        super( initialCapacity );
    }

    public Conditions() {
        super();
    }

    public Conditions(
            @NotNull
                    Collection<? extends Condition> c ) {
        super( c );
    }

    public static Conditions of( Condition... conditions ) {
        Conditions t = new Conditions( conditions.length );
        for ( Condition condition : conditions ) {
            t.add( condition );
        }
        return t;
    }
}
