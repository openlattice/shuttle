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

package com.openlattice.shuttle.edm;

import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.edm.type.Analyzer;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.Set;

public class RequiredProperties {

    private final EdmPrimitiveTypeKind            datatype;
    private final Optional<Boolean>               piiField;
    private final Optional<Analyzer>              analyzer;
    private final Set<PropertyMetadata>           properties;
    private final ImmutableSet<FullQualifiedName> schemas;

    @JsonCreator
    public RequiredProperties(
            @JsonProperty( SerializationConstants.DATATYPE_FIELD ) EdmPrimitiveTypeKind datatype,
            @JsonProperty( SerializationConstants.PII_FIELD ) Optional<Boolean> piiField,
            @JsonProperty( SerializationConstants.ANALYZER ) Optional<Analyzer> analyzer,
            @JsonProperty( SerializationConstants.PROPERTIES_FIELD ) Set<PropertyMetadata> properties ) {

        this.datatype = datatype;
        this.piiField = piiField;
        this.analyzer = analyzer;
        this.properties = properties;
        this.schemas = ImmutableSet.of();
    }

    @JsonProperty( SerializationConstants.SCHEMAS )
    public ImmutableSet<FullQualifiedName> getSchemas() {
        return schemas;
    }

    @JsonProperty( SerializationConstants.DATATYPE_FIELD )
    public EdmPrimitiveTypeKind getDatatype() {
        return datatype;
    }

    @JsonProperty( SerializationConstants.PII_FIELD )
    public Optional<Boolean> getPiiField() {
        return piiField;
    }

    @JsonProperty( SerializationConstants.ANALYZER )
    public Optional<Analyzer> getAnalyzer() {
        return analyzer;
    }

    @JsonProperty( SerializationConstants.PROPERTIES_FIELD )
    public Set<PropertyMetadata> getProperties() {
        return properties;
    }
}
