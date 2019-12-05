package transforms;

import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Parsers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParseIntTransform extends Transformation<String> {

    private static final Logger logger = LoggerFactory
            .getLogger( Shuttle.class );

    /**
     * Represents a transformation to parse integers from a string.
     */
    public ParseIntTransform() {
    }

    @Override
    public Object applyValue( String o ) {
        return Parsers.parseInt( o );
    }
}