package com.openlattice.shuttle.conditionalentities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.openlattice.client.serialization.SerializableFunction;
import com.openlattice.shuttle.EntityDefinition;
import com.openlattice.shuttle.PropertyDefinition;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.openlattice.shuttle.transformations.Transformation.TRANSFORM;

public class Condition  implements Serializable {

    private final String column;
    private final String value;
    private final Boolean reverse;

    /**
     * Represents a transformation to select columns based on non-empty cells.
     * Function goes over columns until a non-zero input is found.
     *
     * @param column:            column to test for value/null
     * @param value:             what value should the column have
     * @param reverse:           if the outcome should be reverse (true -> false)
     */
    @JsonCreator
    public Condition(

            @JsonProperty( Constants.COLUMN ) String column,
            @JsonProperty( Constants.VALUE ) String value,
            @JsonProperty( Constants.REVERSE ) Boolean reverse) {

        this.column = column;
        this.value = value;
        this.reverse = reverse;
    }

    @JsonProperty( value = Constants.COLUMN )
    public String getColumn() {
        return this.column;
    }

    @JsonProperty( value = Constants.VALUE )
    public String getValue() {
        return this.value;
    }

    @JsonProperty( value = Constants.REVERSE )
    public Boolean getReverse() {
        return this.reverse;
    }

        public Boolean apply( Map<String, String> row ) {
        String o = row.get(column);
        Boolean output = false;
        if (!(value == null)){
            if (o == value){
                output = true;
            }
        } else {
            if (StringUtils.isNotBlank( o )){
                output = true;
            }
        }
        if (this.reverse){
            output = !output;
        }
        return output;
    }
    // no builder yet ! Only to be used with json flights !
}
