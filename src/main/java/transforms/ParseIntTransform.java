package transforms;

import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Parsers;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;

public class ParseIntTransform extends Transformation<String> {

    private static final Logger logger = LoggerFactory
            .getLogger(Shuttle.class);

    /**
     * Represents a transformation to parse integers from a string.
     */
    public ParseIntTransform() {
    }

    @Override
    public Object apply(String o) {
        return Parsers.parseInt(o);
    }
}