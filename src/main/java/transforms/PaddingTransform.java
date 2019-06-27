package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.transformations.Transformations;
import com.openlattice.shuttle.util.Cached;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;

public class PaddingTransform extends Transformation<Map<String, String>> {
    private final String pattern;
    private final int length;
    private final boolean pre;
    private final boolean cutoff;

    /**
     * A transform that pads the input string to a certain length by appending or prepending a repeating pattern
     *
     * @param pattern:            pad will be constructed by repeating this string
     * @param length:             target length of resulting string
     * @param pre:                whether to prepend (as opposed to append)
     * @param cutoff:             whether to trim the string to the target size if it's larger
     *
     */
    @JsonCreator
    public PaddingTransform(
            @JsonProperty( Constants.PATTERN ) String pattern,
            @JsonProperty( Constants.LENGTH ) int length,
            @JsonProperty( Constants.PRE ) boolean pre,
            @JsonProperty( Constants.CUTOFF ) boolean cutoff
            ) {
        this.pattern = pattern;
        this.length = length;
        this.pre = pre;
        this.cutoff = cutoff;
    }

    @JsonCreator
    public PaddingTransform(
            @JsonProperty( Constants.PATTERN ) String pattern,
            @JsonProperty( Constants.LENGTH ) int length,
            @JsonProperty( Constants.PRE ) boolean pre
    ) {
        this.pattern = pattern;
        this.length = length;
        this.pre = pre;
        this.cutoff = false;
    }


    @JsonProperty( Constants.PATTERN )
    public String getPattern() {
        return pattern;
    }

    @JsonProperty( Constants.LENGTH )
    public int getLength() {
        return length;
    }

    @JsonProperty( Constants.PRE )
    public boolean getPre() {
        return pre;
    }

    @JsonProperty( Constants.CUTOFF )
    public boolean getCutoff() {
        return cutoff;
    }

    @Override
    public Object applyValue( String o ) {

        if (length < 0) {
            throw new IllegalStateException( "Negative length given." );
        }

        if (pattern.equals("")) {
            throw new IllegalStateException( "Empty pattern given." );
        }

        if (o.length() > length) {
            if (cutoff) {
                if (pre) {
                    return o.substring(o.length() - length);
                }
                else {
                    return o.substring(0, length);
                }
            }
            return o;
        }

        StringBuilder builder = new StringBuilder(o);
        if (pre) {
            while (length > builder.length()) {
                builder.insert(0,pattern);
            }
            return builder.substring(builder.length() - length);
        }
        else {
            while (length > builder.length()) {
                builder.append(pattern);
            }
            return builder.substring(0, length);
        }
    }

}