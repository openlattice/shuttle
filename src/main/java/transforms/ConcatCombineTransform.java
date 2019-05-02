package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

public class ConcatCombineTransform extends Transformation<Map<String, String>> {
    private final List<Transformation> transforms;
    private final String       separator;

    /**
     * Represents a transformation to concatenate values *resulting from other transformations*.
     * The difference with ConcatTransform is that the input is transformations, and the resulting string will be
     * concatenated using the requested separator. Empty cells are skipped.  If all
     * are empty, null is returned.
     *
     * @param transforms:   list of transformations
     * @param separator: separator to concatenate the values
     */
    @JsonCreator
    public ConcatCombineTransform(
            @JsonProperty( Constants.TRANSFORMS ) List<Transformation> transforms,
            @JsonProperty( Constants.SEP ) String separator ) {
        this.transforms = transforms;
        this.separator = separator == null? "-": separator;
    }

    @Override
    public Object apply( Map<String, String> row ) {
        StringBuilder sb = new StringBuilder();
        String sep = "";
        for ( Transformation s : transforms ) {
            Object toadd = s.apply(row);
            if ( toadd != null && !StringUtils.isBlank( toadd.toString()) ) {
                sb.append(sep).append(toadd);
                sep = separator;
            }
        }

        String outstring = sb.toString();
        if ( StringUtils.isBlank( outstring ) ) {
            return null;
        }
        return outstring;
    }

}

