package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.openlattice.shuttle.transformations.Transformation;

public class CaseTransform extends Transformation<String> {


    @JsonCreator
    public CaseTransform() {}

    @Override
    public Object apply( String o ) {
        return o.substring(0, 1).toUpperCase() + o.substring(1).toLowerCase();
    }

 }
