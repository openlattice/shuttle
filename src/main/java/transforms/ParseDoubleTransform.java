package transforms;

import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Parsers;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParseDoubleTransform extends Transformation<String> {

    private static final Logger logger = LoggerFactory
            .getLogger( Shuttle.class );

    /**
     * Represents a transformation to parse doubles from a string.
     */
    public ParseDoubleTransform() {
    }

    @Override
    public Object applyValue( String o ) {
        return Parsers.parseDouble( o );
    }

}
