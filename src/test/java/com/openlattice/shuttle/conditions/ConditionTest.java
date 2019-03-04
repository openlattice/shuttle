package com.openlattice.shuttle.conditions;

import com.openlattice.shuttle.transformations.Transformation;
import conditions.CompareCondition;
import org.junit.Assert;
import org.junit.Test;
import transforms.DateTimeTransform;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ConditionTest {
    String sex             = "f";
    String first           = "John";
    String last            = "Doe";
    String family          = "Joanna Doe (mother)";
    String DOB             = "03/05/1998 10:00";
    String address         = "560 Scott Street, San Francisco, CA 94117";
    String encounter1Start = "03/05/2000 10:00";
    String encounter1End   = "03/06/2000 12:00";
    String encounter2Start = "03/05/2000 09:00";
    String encounter2End   = "03/06/2000 12:00";
    String episode1Start   = "03/05/2000 09:30";
    String episode1End     = "03/06/2000 12:00";
    String episode2Start   = "03/05/2000 09:00";
    String episode2End     = "03/06/2000 12:00";

    public Map<String, String> getTestRow() {
        Map<String, String> testrow = new HashMap<String, String>();
        testrow.put( "FirstName", first );
        testrow.put( "LastName", last );
        testrow.put( "Family", family );
        testrow.put( "DOB", DOB );
        testrow.put( "Encounter 1 start date", encounter1Start );
        testrow.put( "Encounter 1 end date", encounter1End );
        testrow.put( "Encounter 2 start date", encounter2Start );
        testrow.put( "Encounter 2 end date", encounter2End );
        testrow.put( "Episode 1 start date", episode1Start );
        testrow.put( "Episode 1 end date", episode1End );
        testrow.put( "Episode 2 start date", episode2Start );
        testrow.put( "Episode 2 end date", episode2End );
        testrow.put( "Sex", sex );
        testrow.put( "Address", address );
        return testrow;
    }

    //==================//
    // BOOLEAN TESTS    //
    //==================//

    @Test
    public void testCompareCondition() {
        Map<String, String> row = getTestRow();
        String[] datePattern = { "MM/dd/yyyy HH:mm" };
        ArrayList<Transformation> transforms = new ArrayList<>();
        transforms.add( new DateTimeTransform( datePattern, null ) );
        CompareCondition comp1 = new CompareCondition(
                "Encounter 1 start date",
                transforms,
                "Episode 1 start date",
                transforms, CompareCondition.Comparison.lt
        );

        CompareCondition comp2 = new CompareCondition(
                "Encounter 2 end date",
                transforms,
                "Episode 1 end date",
                transforms, CompareCondition.Comparison.eq
        );
        Assert.assertEquals( comp1.apply( row ), false );
        Assert.assertEquals( comp2.apply( row ), true );
    }
}