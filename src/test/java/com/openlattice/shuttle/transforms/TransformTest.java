package com.openlattice.shuttle.transforms;

import com.openlattice.shuttle.transformations.Transformations;
import org.junit.Assert;
import org.junit.Test;
import transforms.*;

import javax.management.AttributeList;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

public class TransformTest {

    public Map<String, String> getTestRow() {
        Map<String, String> testrow = new HashMap<String, String>();
        testrow.put("FirstName", "John");
        testrow.put("LastName", "Doe");
        testrow.put("DOB","03/05/1998 10:00" );
        testrow.put("SSN", null);
        return testrow;
    }

    public Transformations getTrueTestTransforms() {
        Transformations transfos = new Transformations();
        transfos.add( new ValueTransform("yup")) ;
        return transfos;
    }

    public Transformations getFalseTestTransforms() {
        Transformations transfos = new Transformations();
        transfos.add( new ValueTransform("nope")) ;
        return transfos;
    }

    @Test
    public void testBooleanContainsTransform() {
        Optional<Boolean> optrue = Optional.of(true);
        Optional<Boolean> opfals = Optional.of(false);
        Optional<Transformations> truetransfo = Optional.of(getTrueTestTransforms());
        Optional<Transformations> falsetransfo = Optional.of(getFalseTestTransforms());
        Object booleanContainsTest1 = new BooleanContainsTransform(
                "DOB",
                "1998",
                optrue,
                truetransfo,
                falsetransfo
        ).apply(getTestRow());
        Assert.assertEquals("yup",booleanContainsTest1);

        Object booleanContainsTest2 = new BooleanContainsTransform(
                "DOB",
                "1999",
                optrue,
                truetransfo,
                falsetransfo
        ).apply(getTestRow());
        Assert.assertEquals("nope",booleanContainsTest2);

        Object booleanContainsTest3 = new BooleanContainsTransform(
                "FirstName",
                "jOHN",
                opfals,
                truetransfo,
                falsetransfo
        ).apply(getTestRow());
        Assert.assertEquals("nope",booleanContainsTest3);

    }

    @Test
    public void testBooleanIsNullTransform() {
        Optional<Boolean> blue = Optional.of(true);
        Optional<Transformations> truetransfo = Optional.of(getTrueTestTransforms());
        Optional<Transformations> falsetransfo = Optional.of(getFalseTestTransforms());
        Object booleanIsNullTest1 = new BooleanIsNullTransform(
                "SSN",
                truetransfo,
                falsetransfo
        ).apply(getTestRow());
        Assert.assertEquals("yup",booleanIsNullTest1);

        Object booleanIsNullTest2 = new BooleanIsNullTransform(
                "DOB",
                truetransfo,
                falsetransfo
        ).apply(getTestRow());
        Assert.assertEquals("nope",booleanIsNullTest2);
    }

    @Test
    public void testBooleanPrefixTransform() {

        Optional<Boolean> optrue = Optional.of(true);
        Optional<Boolean> opfals = Optional.of(false);
        Optional<Transformations> truetransfo = Optional.of(getTrueTestTransforms());
        Optional<Transformations> falsetransfo = Optional.of(getFalseTestTransforms());

        // test true with ignorecase = true
        Object booleanPrefixTest1 = new BooleanPrefixTransform(
                "jo",
                "FirstName",
                optrue,
                truetransfo,
                falsetransfo
        ).apply(getTestRow());
        Assert.assertEquals("yup",booleanPrefixTest1);

        // test true with ignorecase = true
        Object booleanPrefixTest2 = new BooleanPrefixTransform(
                "jo",
                "FirstName",
                opfals,
                truetransfo,
                falsetransfo
        ).apply(getTestRow());
        Assert.assertEquals("nope",booleanPrefixTest2);

        // test false with ignorecase = true
        Object booleanPrefixTest3 = new BooleanPrefixTransform(
                "Nobody",
                "FirstName",
                opfals,
                truetransfo,
                falsetransfo
        ).apply(getTestRow());
        Assert.assertEquals("nope",booleanPrefixTest3);

    }

    @Test
    public void testBooleanRegexTransform() {
        Optional<Transformations> truetransfo = Optional.of(getTrueTestTransforms());
        Optional<Transformations> falsetransfo = Optional.of(getFalseTestTransforms());

        // test true with ignorecase = true
        Object booleanRegexTest1 = new BooleanRegexTransform(
                "FirstName",
                "nobody|Jo",
                truetransfo,
                falsetransfo
        ).apply(getTestRow());
        Assert.assertEquals("yup",booleanRegexTest1);
    }

    @Test
    public void testPrefixTransform() {
        Object prefixTest1 = new PrefixTransform( "prefix_" ).apply( "name" );
        Assert.assertEquals( "prefix_name", prefixTest1 );

        Object prefixTest2 = new PrefixTransform( "prefix_" ).apply( null );
        Assert.assertEquals( null, prefixTest2 );
    }

    @Test
    public void testParseIntTransform() {
        Object parseIntTest1 = new ParseIntTransform().apply( "3" );
        Assert.assertEquals( 3, parseIntTest1 );
    }

    @Test
    public void testDateTimeTransform() {
        String[] patterns = { "MM/dd/yyyy HH:mm" };
        OffsetDateTime expected = OffsetDateTime
                .of( LocalDateTime.of( 1998, 03, 05, 10, 0 ), ZoneOffset.ofHours( -5 ) );
        Object dateTimeTest1 = new DateTimeTransform( patterns ).apply( getTestRow().get("DOB") );
        Assert.assertEquals( expected, dateTimeTest1 );
    }

    @Test
    public void testConcatTransform() {
        String expected = "John Doe";
        List<String> cols = Arrays.asList("FirstName","LastName");
        Object concatTest1 = new ConcatTransform(cols, " ").apply(getTestRow());
        Assert.assertEquals( expected, concatTest1 );
    }

}
