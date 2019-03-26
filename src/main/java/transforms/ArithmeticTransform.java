package transforms;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;

import java.util.Map;
import java.util.List;

public class ArithmeticTransform extends Transformation<Map<String, String>> {
    private String operator;
    private List<Transformation> leftTransforms, rightTransforms;

    public ArithmeticTransform(
            @JsonProperty( Constants.LEFTTRANSFORMS ) List<Transformation> leftTransforms,
            @JsonProperty( Constants.RIGHTTRANSFORMS ) List<Transformation> rightTransforms,
            @JsonProperty( Constants.OPERATOR ) String operator
            ) {
        this.operator = operator;
        this.leftTransforms = leftTransforms;
        this.rightTransforms = rightTransforms;
    }

    @JsonProperty( Constants.OPERATOR )
    public String getOperator() {
        return operator;
    }

    @JsonProperty( Constants.LEFTTRANSFORMS )
    public List<Transformation> getLeftTransforms() {
        return leftTransforms;
    }

    @JsonProperty( Constants.RIGHTTRANSFORMS )
    public List<Transformation> getRightTransforms() {
        return rightTransforms;
    }

    @Override
    public Object apply( Map<String, String> row ) {
        Object leftTransformed = row;
        for ( Transformation t : leftTransforms ) {
            leftTransformed = t.apply( leftTransformed );
        }

        Object rightTransformed = row;
        for ( Transformation t : rightTransforms ) {
            rightTransformed = t.apply( rightTransformed );
        }

        double left, right;

        if ( leftTransformed == null ) {
            left = 0;
        }
        else
        {
            String leftString = leftTransformed.toString();
            if (leftString.equals(""))
            {
                left = 0;
            }
            else
            {
                left = Double.parseDouble(leftString);
            }
        }

        if ( rightTransformed == null ) {
            right = 0;
        }
        else
        {
            String rightString = rightTransformed.toString();
            if (rightString.equals(""))
            {
                right = 0;
            }
            else
            {
                right = Double.parseDouble(rightString);
            }
        }

        switch ( operator ) {
            case "+":
                return left + right;
            case "-":
                return left - right;
            case "*":
                return left * right;
            case "/":
                return left / right;
            default:
                return null;
        }
    }

}
