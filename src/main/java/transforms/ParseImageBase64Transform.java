package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

import java.util.Base64;
import java.util.Objects;

public class ParseImageBase64Transform extends Transformation<String> {


    /**
     * Represents a transformation to encode a string as base64 image
     *
     */
    @JsonCreator
    public ParseImageBase64Transform() {}

    @Override
    public Object apply( String o ) {
        return Base64.getEncoder().encode(o.getBytes());
    }
}
