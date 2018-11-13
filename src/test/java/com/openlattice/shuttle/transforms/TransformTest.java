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

    String lat               = "36.23452";
    String lon               = "30.34573";
    String first             = "John";
    String last              = "Doe";
    String DOB               = "03/05/1998 10:00";
    String address           = "560 Scott Street, San Francisco, CA 94117";
    String dateArrest        = "10/01/92";
    String dateRelease       = "10-01-25";
    String datetimeCommitted = "03/05/00 10:00";

    public Map<String, String> getTestRow() {
        Map<String, String> testrow = new HashMap<String, String>();
        testrow.put( "FirstName", first );
        testrow.put( "LastName", last );
        testrow.put( "DOB", DOB );
        testrow.put( "ArrestedDate", dateArrest );
        testrow.put( "ReleasedDate", dateRelease );
        testrow.put( "SSN", null );
        testrow.put( "Address", address );
        testrow.put( "CommittedDateTime", datetimeCommitted );
        testrow.put( "Latt", lat );
        testrow.put( "Long", lon );
        return testrow;
    }

    public Transformations getTrueTestTransforms() {
        Transformations transfos = new Transformations();
        transfos.add( new ValueTransform( "yup" ) );
        return transfos;
    }

    public Transformations getFalseTestTransforms() {
        Transformations transfos = new Transformations();
        transfos.add( new ValueTransform( "nope" ) );
        return transfos;
    }

    @Test
    public void testGeographyPointTransform() {
        Transformations latTransfos = new Transformations();
        latTransfos.add( new ColumnTransform( "Latt" ) );
        Transformations lonTransfos = new Transformations();
        lonTransfos.add( new ColumnTransform( "Long" ) );
        Object geographypointTest1 = new GeographyPointTransform(
                latTransfos, lonTransfos
        ).apply( getTestRow() );
        Assert.assertEquals( lat + "," + lon, geographypointTest1 );

    }

    @Test
    public void testBooleanContainsTransform() {
        Optional<Boolean> optrue = Optional.of( true );
        Optional<Boolean> opfals = Optional.of( false );
        Optional<Transformations> truetransfo = Optional.of( getTrueTestTransforms() );
        Optional<Transformations> falsetransfo = Optional.of( getFalseTestTransforms() );
        Object booleanContainsTest1 = new BooleanContainsTransform(
                "DOB",
                "1998",
                optrue,
                truetransfo,
                falsetransfo
        ).apply( getTestRow() );
        Assert.assertEquals( "yup", booleanContainsTest1 );

        Object booleanContainsTest2 = new BooleanContainsTransform(
                "DOB",
                "1999",
                optrue,
                truetransfo,
                falsetransfo
        ).apply( getTestRow() );
        Assert.assertEquals( "nope", booleanContainsTest2 );

        Object booleanContainsTest3 = new BooleanContainsTransform(
                "FirstName",
                "jOHN",
                opfals,
                truetransfo,
                falsetransfo
        ).apply( getTestRow() );
        Assert.assertEquals( "nope", booleanContainsTest3 );

    }

    @Test
    public void testBooleanIsNullTransform() {
        Optional<Boolean> blue = Optional.of( true );
        Optional<Transformations> truetransfo = Optional.of( getTrueTestTransforms() );
        Optional<Transformations> falsetransfo = Optional.of( getFalseTestTransforms() );
        Object booleanIsNullTest1 = new BooleanIsNullTransform(
                "SSN",
                truetransfo,
                falsetransfo
        ).apply( getTestRow() );
        Assert.assertEquals( "yup", booleanIsNullTest1 );

        Object booleanIsNullTest2 = new BooleanIsNullTransform(
                "DOB",
                truetransfo,
                falsetransfo
        ).apply( getTestRow() );
        Assert.assertEquals( "nope", booleanIsNullTest2 );
    }

    @Test
    public void testBooleanPrefixTransform() {

        Optional<Boolean> optrue = Optional.of( true );
        Optional<Boolean> opfals = Optional.of( false );
        Optional<Transformations> truetransfo = Optional.of( getTrueTestTransforms() );
        Optional<Transformations> falsetransfo = Optional.of( getFalseTestTransforms() );

        // test true with ignorecase = true
        Object booleanPrefixTest1 = new BooleanPrefixTransform(
                "jo",
                "FirstName",
                optrue,
                truetransfo,
                falsetransfo
        ).apply( getTestRow() );
        Assert.assertEquals( "yup", booleanPrefixTest1 );

        // test true with ignorecase = true
        Object booleanPrefixTest2 = new BooleanPrefixTransform(
                "jo",
                "FirstName",
                opfals,
                truetransfo,
                falsetransfo
        ).apply( getTestRow() );
        Assert.assertEquals( "nope", booleanPrefixTest2 );

        // test false with ignorecase = true
        Object booleanPrefixTest3 = new BooleanPrefixTransform(
                "Nobody",
                "FirstName",
                opfals,
                truetransfo,
                falsetransfo
        ).apply( getTestRow() );
        Assert.assertEquals( "nope", booleanPrefixTest3 );

    }

    @Test
    public void testBooleanRegexTransform() {
        Optional<Transformations> truetransfo = Optional.of( getTrueTestTransforms() );
        Optional<Transformations> falsetransfo = Optional.of( getFalseTestTransforms() );

        // test true with ignorecase = true
        Object booleanRegexTest1 = new BooleanRegexTransform(
                "FirstName",
                "nobody|Jo",
                truetransfo,
                falsetransfo
        ).apply( getTestRow() );
        Assert.assertEquals( "yup", booleanRegexTest1 );
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
    public void testParseBooleanTransform() {
        Object parseBoolTest1 = new ParseBoolTransform().apply( "1" );
        Assert.assertEquals( true, parseBoolTest1 );
        Object parseBoolTest2 = new ParseBoolTransform().apply( "0" );
        Assert.assertEquals( false, parseBoolTest2 );
        Object parseBoolTest3 = new ParseBoolTransform().apply( "true" );
        Assert.assertEquals( true, parseBoolTest3 );
        Object parseBoolTest4 = new ParseBoolTransform().apply( "false" );
        Assert.assertEquals( false, parseBoolTest4 );
    }

    @Test
    public void testDateTimeTransform() {
        String[] patterns = { "MM/dd/yyyy HH:mm", "MM/dd/yy HH:mm" };
        OffsetDateTime expected1 = OffsetDateTime
                .of( LocalDateTime.of( 1998, 03, 05, 10, 0 ), ZoneOffset.ofHours( -5 ) );
        Object dateTimeTest1 = new DateTimeTransform( patterns ).apply( getTestRow().get( "DOB" ) );
        Assert.assertEquals( expected1, dateTimeTest1 );
        OffsetDateTime expected2 = OffsetDateTime
                .of( LocalDateTime.of( 2000, 03, 05, 10, 0 ), ZoneOffset.ofHours( -5 ) );
        Object dateTimeTest2 = new DateTimeTransform( patterns ).apply( getTestRow().get( "CommittedDateTime" ) );
        Assert.assertEquals( expected1, dateTimeTest1 );
        Assert.assertEquals( expected2, dateTimeTest2 );
    }

    @Test
    public void testDateTransform() {
        String[] patterns = { "MM/dd/yy", "MM-dd-yy" };
        LocalDate expected1 = LocalDate.of( 1992, 10, 01 );
        Object dateTimeTest1 = new DateTransform( patterns ).apply( getTestRow().get( "ArrestedDate" ) );
        Assert.assertEquals( expected1, dateTimeTest1 );
        LocalDate expected2 = LocalDate.of( 2025, 10, 01 );
        Object dateTimeTest2 = new DateTransform( patterns ).apply( getTestRow().get( "ReleasedDate" ) );
        Assert.assertEquals( expected2, dateTimeTest2 );
    }

    @Test
    public void testConcatTransform() {
        String expected = "John Doe";
        List<String> cols = Arrays.asList( "FirstName", "LastName" );
        Object concatTest1 = new ConcatTransform( cols, " " ).apply( getTestRow() );
        Assert.assertEquals( expected, concatTest1 );
    }

    @Test
    public void testGeocoderTransform() {
        String expectedStreet = "Scott Street";
        Object geocoderTest1 = new GeocoderTransform( "road", Optional.empty() )
                .applyValue( getTestRow().get( "Address" ) );
        String expectedNo = "560";
        Object geocoderTest2 = new GeocoderTransform( "house_number", Optional.empty() )
                .applyValue( getTestRow().get( "Address" ) );
        String expectedType = "house";
        Object geocoderTest3 = new GeocoderTransform( "type", Optional.empty() )
                .applyValue( getTestRow().get( "Address" ) );
        Assert.assertEquals( expectedStreet, geocoderTest1 );
        Assert.assertEquals( expectedNo, geocoderTest2 );
        Assert.assertEquals( expectedType, geocoderTest3 );
    }

}
