package transforms;

import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.transformations.Transformation;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;

public class ParseIntTransform extends Transformation<String> {

    private static final Logger logger = LoggerFactory
            .getLogger( Shuttle.class );

    /**
     * Represents a transformation to parse integers from a string.
     */
    public ParseIntTransform() {
    }

    @Override
    public Object apply( String o ) {
        if ( StringUtils.isNotBlank( o ) ) {
            try {
                Integer i = Integer.parseInt( o );
                return i.toString();
            } catch ( NumberFormatException e ) {
            }
            try {
                Double d = Double.parseDouble( o );
                BigInteger k = BigDecimal.valueOf( d ).toBigInteger();
                return k.toString();
            } catch ( NumberFormatException f ) {
            }
        }
        logger.error( "Unable to parse int from value {}", o );
        return null;
    }

}