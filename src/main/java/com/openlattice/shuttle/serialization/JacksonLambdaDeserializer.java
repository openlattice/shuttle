/*
 * Copyright (C) 2017. OpenLattice, Inc
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
 */

package com.openlattice.shuttle.serialization;

import com.openlattice.client.serialization.SerializableFunction;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.invoke.SerializedLambda;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.function.Function;

public class JacksonLambdaDeserializer extends StdDeserializer<SerializableFunction> {

    private static       Decoder decoder = Base64.getDecoder();
    private static final Encoder encoder = Base64.getEncoder();

    private static final ThreadLocal<Kryo> kryo = ThreadLocal.withInitial( () -> {

        Kryo kryo = new Kryo();

        // https://github.com/EsotericSoftware/kryo/blob/master/test/com/esotericsoftware/kryo/serializers/Java8ClosureSerializerTest.java
        kryo.setInstantiatorStrategy(
                new Kryo.DefaultInstantiatorStrategy(
                        new StdInstantiatorStrategy()
                )
        );

        kryo.register( Object[].class );
        kryo.register( Class.class );

        // Shared Lambdas
        kryo.register( SerializableFunction.class );
        kryo.register( SerializedLambda.class );

        // always needed for closure serialization, also if
        // registrationRequired=false
        kryo.register(
                ClosureSerializer.Closure.class,
                new ClosureSerializer()
        );

        kryo.register(
                Function.class,
                new ClosureSerializer()
        );

        return kryo;
    } );

    public static void registerWithMapper( ObjectMapper mapper ) {

        SimpleModule module = new SimpleModule();
        module.addDeserializer( SerializableFunction.class, new JacksonLambdaDeserializer() );
        mapper.registerModule( module );
    }

    public JacksonLambdaDeserializer() {
        super( SerializableFunction.class );
    }

    @Override public SerializableFunction deserialize( JsonParser p, DeserializationContext ctxt )
            throws IOException, JsonProcessingException {

        String encodedBytes = p.readValueAs( String.class );
        byte[] bytes = decoder.decode( encodedBytes );
        ByteArrayInputStream input = new ByteArrayInputStream( bytes );

        return (SerializableFunction) kryo.get().readClassAndObject( new Input( input ) );
    }
}
