/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.shuttle.transforms;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test cases for Jackson Polymorphic Deserialization
 */

@Ignore
public class TestCase {
    public static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.registerModule( new Jdk8Module() );
    }

    @Test
    public void testListWithoutOptional() throws IOException {
        //Fails
        ConcreteExample1 ex1 = new ConcreteExample1();
        ConcreteExample2 ex2 = new ConcreteExample2();
        ex1.a = "another";
        ex2.b = 1;
        String json = mapper.writeValueAsString( Lists.newArrayList( ex1, ex2 ) );
        List<AbstractExample> examples = mapper.readValue( json, new TypeReference<List<AbstractExample>>() {
        } );
        Assert.assertEquals( ex1, examples.get( 0 ) );
        Assert.assertEquals( ex2, examples.get( 1 ) );
    }

    @Test
    public void testMarkedListWithoutOptional() throws IOException {
        //Fails
        ConcreteExample1 ex1 = new ConcreteExample1();
        ConcreteExample2 ex2 = new ConcreteExample2();
        ex1.a = "another";
        ex2.b = 1;
        ListAbstractExamples lae = new ListAbstractExamples();
        lae.add( ex1 );
        lae.add( ex2 );
        String json = mapper.writeValueAsString( lae );
        List<AbstractExample> examples = mapper.readValue( json, ListAbstractExamples.class );
        Assert.assertEquals( ex1, examples.get( 0 ) );
        Assert.assertEquals( ex2, examples.get( 1 ) );
    }

    @Test
    public void testWrappedListWithoutOptional() throws IOException {
        //Fails
        ConcreteExample1 ex1 = new ConcreteExample1();
        ConcreteExample2 ex2 = new ConcreteExample2();
        ex1.a = "another";
        ex2.b = 1;
        ExampleList exl = new ExampleList();
        exl.examples = new ArrayList<>();
        exl.examples.add( ex1 );
        exl.examples.add( ex2 );
        String json = mapper.writeValueAsString( exl );
        ExampleList actualExl = mapper.readValue( json, ExampleList.class );
        Assert.assertEquals( ex1, actualExl.examples.get( 0 ) );
        Assert.assertEquals( ex2, actualExl.examples.get( 1 ) );
    }

    @Test
    public void testWrappedListWithOptional() throws IOException {
        //Fails
        ConcreteExample1 ex1 = new ConcreteExample1();
        ConcreteExample2 ex2 = new ConcreteExample2();
        ex1.a = "another";
        ex2.b = 1;
        ExampleOptionalList exol = new ExampleOptionalList();
        ExampleList exl = new ExampleList();
        exl.examples = new ArrayList<>();
        exl.examples.add( ex1 );
        exl.examples.add( ex2 );
        exol.examples = Optional.of( exl.examples );
        String json = mapper.writeValueAsString( exol );
        ExampleOptionalList actualExol = mapper.readValue( json, ExampleOptionalList.class );
        Assert.assertEquals( ex1, actualExol.examples.get().get( 0 ) );
        Assert.assertEquals( ex2, actualExol.examples.get().get( 1 ) );
    }

    @JsonTypeInfo( use = Id.CLASS, include = As.PROPERTY )
    public static abstract class AbstractExample<T> implements Function<T, Object> {
    }

    public static class ListAbstractExamples extends ArrayList<AbstractExample> {
    }

    public static class ExampleList {
        public List<AbstractExample> examples;
    }

    public static class ExampleOptionalList {
        public Optional<List<AbstractExample>> examples;
    }

    public static class ConcreteExample1 extends AbstractExample<String> {
        @JsonProperty( "a" )
        public String a;

        @Override public Object apply( String s ) {
            return s + a;
        }

        @Override public boolean equals( Object o ) {

            if ( this == o ) { return true; }
            if ( !( o instanceof ConcreteExample1 ) ) { return false; }
            ConcreteExample1 that = (ConcreteExample1) o;
            return Objects.equals( a, that.a );
        }

        @Override public String toString() {
            return "ConcreteExample1{" +
                    "a='" + a + '\'' +
                    '}';
        }

        @Override public int hashCode() {

            return Objects.hash( a );
        }
    }

    public static class ConcreteExample2 extends AbstractExample<Integer> {
        @JsonProperty( "b" )
        public Integer b;

        @Override public Object apply( Integer integer ) {
            return integer + b;
        }

        @Override public String toString() {
            return "ConcreteExample2{" +
                    "b=" + b +
                    '}';
        }

        @Override public boolean equals( Object o ) {
            if ( this == o ) { return true; }
            if ( !( o instanceof ConcreteExample2 ) ) { return false; }
            ConcreteExample2 that = (ConcreteExample2) o;
            return Objects.equals( b, that.b );
        }

        @Override public int hashCode() {

            return Objects.hash( b );
        }
    }

}
