package com.openlattice.shuttle.transforms;

import org.junit.Assert;
import org.junit.Test;
import transforms.DateTimeTransform;
import transforms.ParseIntTransform;
import transforms.PrefixTransform;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class TransformTest {

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
                .of( LocalDateTime.of( 2018, 03, 05, 10, 0 ), ZoneOffset.ofHours( -5 ) );
        Object dateTimeTest1 = new DateTimeTransform( patterns ).apply( "03/05/2018 10:00" );
        Assert.assertEquals( expected, dateTimeTest1 );
    }

}
