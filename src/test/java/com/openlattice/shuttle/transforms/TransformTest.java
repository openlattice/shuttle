package com.openlattice.shuttle.transforms;

import com.openlattice.shuttle.transformations.Transformations;
import org.junit.Assert;
import org.junit.Test;
import transforms.*;

import javax.management.AttributeList;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

public class TransformTest {

    public Map<String, String> getTestRow() {
        Map<String, String> testrow = new HashMap<String, String>();
        testrow.put("FirstName", "John");
        testrow.put("LastName", "Doe");
        testrow.put("DOB", "03/05/1998 10:00");
        testrow.put("ArrestedDate", "10/01/92");
        testrow.put("ReleasedDate", "10-01-25");
        testrow.put("SSN", null);
        testrow.put("Sex", "f");
        testrow.put("Address", "560 Scott Street, San Francisco, CA 94117");
        testrow.put("CommittedDateTime", "03/05/00 10:00");
        return testrow;
    }

    //==================//
    // HELPER FUNCTIONS //
    //==================//

    public Transformations getTrueTestTransforms() {
        Transformations transfos = new Transformations();
        transfos.add(new ValueTransform("yup"));
        return transfos;
    }

    public Transformations getFalseTestTransforms() {
        Transformations transfos = new Transformations();
        transfos.add(new ValueTransform("nope"));
        return transfos;
    }

    //==================//
    // BOOLEAN TESTS    //
    //==================//

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
        Assert.assertEquals("yup", booleanContainsTest1);

        Object booleanContainsTest2 = new BooleanContainsTransform(
                "DOB",
                "1999",
                optrue,
                truetransfo,
                falsetransfo
        ).apply(getTestRow());
        Assert.assertEquals("nope", booleanContainsTest2);

        Object booleanContainsTest3 = new BooleanContainsTransform(
                "FirstName",
                "jOHN",
                opfals,
                truetransfo,
                falsetransfo
        ).apply(getTestRow());
        Assert.assertEquals("nope", booleanContainsTest3);

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
        Assert.assertEquals("yup", booleanIsNullTest1);

        Object booleanIsNullTest2 = new BooleanIsNullTransform(
                "DOB",
                truetransfo,
                falsetransfo
        ).apply(getTestRow());
        Assert.assertEquals("nope", booleanIsNullTest2);
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
        Assert.assertEquals("yup", booleanPrefixTest1);

        // test true with ignorecase = true
        Object booleanPrefixTest2 = new BooleanPrefixTransform(
                "jo",
                "FirstName",
                opfals,
                truetransfo,
                falsetransfo
        ).apply(getTestRow());
        Assert.assertEquals("nope", booleanPrefixTest2);

        // test false with ignorecase = true
        Object booleanPrefixTest3 = new BooleanPrefixTransform(
                "Nobody",
                "FirstName",
                opfals,
                truetransfo,
                falsetransfo
        ).apply(getTestRow());
        Assert.assertEquals("nope", booleanPrefixTest3);

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
        Assert.assertEquals("yup", booleanRegexTest1);
    }

    //==================//
    // PARSING TESTS    //
    //==================//

    @Test
    public void testParseIntTransform() {
        Object parseIntTest1 = new ParseIntTransform().apply("3");
        Assert.assertEquals(3, parseIntTest1);
    }

    @Test
    public void testParseBooleanTransform() {
        Object parseBoolTest1 = new ParseBoolTransform().apply("1");
        Assert.assertEquals(true, parseBoolTest1);
        Object parseBoolTest2 = new ParseBoolTransform().apply("0");
        Assert.assertEquals(false, parseBoolTest2);
        Object parseBoolTest3 = new ParseBoolTransform().apply("true");
        Assert.assertEquals(true, parseBoolTest3);
        Object parseBoolTest4 = new ParseBoolTransform().apply("false");
        Assert.assertEquals(false, parseBoolTest4);
    }

    //==================//
    // DATETIME TESTS   //
    //==================//

    @Test
    public void testDateTimeTransform() {
        String[] patterns = {"MM/dd/yyyy HH:mm", "MM/dd/yy HH:mm"};
        OffsetDateTime expected1 = OffsetDateTime
                .of(LocalDateTime.of(1998, 03, 05, 10, 0), ZoneOffset.ofHours(-5));
        Object dateTimeTest1 = new DateTimeTransform(patterns).apply(getTestRow().get("DOB"));
        Assert.assertEquals(expected1, dateTimeTest1);
        OffsetDateTime expected2 = OffsetDateTime
                .of(LocalDateTime.of(2000, 03, 05, 10, 0), ZoneOffset.ofHours(-5));
        Object dateTimeTest2 = new DateTimeTransform(patterns).apply(getTestRow().get("CommittedDateTime"));
        Assert.assertEquals(expected1, dateTimeTest1);
        Assert.assertEquals(expected2, dateTimeTest2);
    }

    @Test
    public void testDateTransform() {
        String[] patterns = {"MM/dd/yy", "MM-dd-yy"};
        LocalDate expected1 = LocalDate.of(1992, 10, 01);
        Object dateTimeTest1 = new DateTransform(patterns).apply(getTestRow().get("ArrestedDate"));
        Assert.assertEquals(expected1, dateTimeTest1);
        LocalDate expected2 = LocalDate.of(2025, 10, 01);
        Object dateTimeTest2 = new DateTransform(patterns).apply(getTestRow().get("ReleasedDate"));
        Assert.assertEquals(expected2, dateTimeTest2);
    }


    //==================//
    // OTHER TESTS      //
    //==================//

    @Test
    public void testPrefixTransform() {
        Object prefixTest1 = new PrefixTransform("prefix_").apply("name");
        Assert.assertEquals("prefix_name", prefixTest1);

        Object prefixTest2 = new PrefixTransform("prefix_").apply(null);
        Assert.assertEquals(null, prefixTest2);
    }


    @Test
    public void testConcatTransform() {
        String expected = "John Doe";
        List<String> cols = Arrays.asList("FirstName", "LastName");
        Object concatTest1 = new ConcatTransform(cols, " ").apply(getTestRow());
        Assert.assertEquals(expected, concatTest1);
    }

    @Test
    public void testReplaceTransform() {
        Optional<Boolean> optrue = Optional.of(true);
        Optional<Boolean> opfals = Optional.of(false);

        List<String> target = Arrays.asList("F");
        List<String> goal = Arrays.asList("female");
        // not case sensitive
        Object replaceTest1 = new ReplaceTransform(target, optrue, opfals, goal,"null").apply(getTestRow().get("Sex"));
        Assert.assertEquals("female", replaceTest1);

        // case sensitive
        Object replaceTest2 = new ReplaceTransform(target, opfals, opfals, goal,"null").apply(getTestRow().get("Sex"));
        Assert.assertEquals(null, replaceTest2);

        // return original when valueElse is not specified
        Object replaceTest3 = new ReplaceTransform(target, opfals, opfals, goal,null).apply(getTestRow().get("Sex"));
        Assert.assertEquals("f", replaceTest3);

        List<String> target4 = Arrays.asList("F", "e");
        List<String> goal4 = Arrays.asList("female", "erel");
        Object replaceTest4 = new ReplaceTransform(target4, optrue, optrue, goal4,null).apply(getTestRow().get("Sex"));
        Assert.assertEquals("female", replaceTest4);

        List<String> target5 = Arrays.asList("Android", "a");
        List<String> goal5 = Arrays.asList("Windows", "u");
        Object replaceTest5 = new ReplaceTransform(target5, optrue, optrue, goal5,null).apply("Android gave new life to java");
        Assert.assertEquals("Windows guve new life to juvu", replaceTest5);

    }

    @Test
    public void testGeocoderTransform() {
        String expectedStreet = "Scott Street";
        Object geocoderTest1 = new GeocoderTransform("road", Optional.empty()).applyValue(getTestRow().get("Address"));
        String expectedNo = "560";
        Object geocoderTest2 = new GeocoderTransform("house_number", Optional.empty()).applyValue(getTestRow().get("Address"));
        String expectedType = "house";
        Object geocoderTest3 = new GeocoderTransform("type", Optional.empty()).applyValue(getTestRow().get("Address"));
        Assert.assertEquals(expectedStreet, geocoderTest1);
        Assert.assertEquals(expectedNo, geocoderTest2);
        Assert.assertEquals(expectedType, geocoderTest3);
    }

}
