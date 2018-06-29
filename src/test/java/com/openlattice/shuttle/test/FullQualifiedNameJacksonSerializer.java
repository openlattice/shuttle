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

/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 */

package com.openlattice.shuttle.test;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.data.serializers.FullQualifiedNameJacksonDeserializer;
import java.io.IOException;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

public class FullQualifiedNameJacksonSerializer extends StdSerializer<FullQualifiedName> {
    private static final long         serialVersionUID = 642017294181795076L;
    private static final SimpleModule module           = new SimpleModule( FullQualifiedNameJacksonSerializer.class
            .getName() );

    static {
        module.addSerializer( FullQualifiedName.class, new FullQualifiedNameJacksonSerializer() );
        module.addDeserializer( FullQualifiedName.class, new FullQualifiedNameJacksonDeserializer() );
    }

    public FullQualifiedNameJacksonSerializer() {
        this( FullQualifiedName.class );
    }

    public FullQualifiedNameJacksonSerializer( Class<FullQualifiedName> clazz ) {
        super( clazz );
    }

    @Override
    public void serialize( FullQualifiedName value, JsonGenerator jgen, SerializerProvider provider )
            throws IOException {
        jgen.writeStartObject();
        jgen.writeStringField( SerializationConstants.NAMESPACE_FIELD, value.getNamespace() );
        jgen.writeStringField( SerializationConstants.NAME_FIELD, value.getName() );
        jgen.writeEndObject();
    }

    public static void registerWithMapper( ObjectMapper mapper ) {
        mapper.registerModule( module );
    }
}
