package transforms;

import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Parsers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

public class ParseGeographyPointTransform extends Transformation<String> {
    private static final Pattern geographyPointRegex = Pattern.compile("(\\-)?[0-9]+(\\.){1}[0-9]+(\\,){1}(\\-)?[0-9]+(\\.){1}[0-9]+");
    private static final   Logger  logger              = LoggerFactory
            .getLogger( Shuttle.class );

    /**
     * Represents a transformation to parse a geographypoint from a string.
     */
    public ParseGeographyPointTransform() {
    }

    @Override
    public Object applyValue( String o ) {
        if ( o instanceof String && geographyPointRegex.matcher( ( String ) o ).matches() ) {
            return o;
        } else {
            logger.error("Unable to parse geographypoint, please see debug log for additional information: {}.", o);
        }

        return Parsers.parseInt( o );
    }
}
