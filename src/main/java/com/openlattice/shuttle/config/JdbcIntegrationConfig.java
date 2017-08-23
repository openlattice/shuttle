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
 *
 */

package com.openlattice.shuttle.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class JdbcIntegrationConfig {
    private final String url;

    private final String dbUser;
    private final String dbPassword;

    private final String olsUser;
    private final String olsPassword;

    @JsonCreator
    public JdbcIntegrationConfig(
            @JsonProperty( "url" ) String url,
            @JsonProperty( "dbUser" ) String dbUser,
            @JsonProperty( "dbPassword" ) String dbPassword,
            @JsonProperty( "olsUser" ) String olsUser,
            @JsonProperty( "olsPassword" ) String olsPassword ) {
        this.url = url;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
        this.olsUser = olsUser;
        this.olsPassword = olsPassword;
    }

    public String getUrl() {
        return url;
    }

    public String getDbUser() {
        return dbUser;
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public String getOlsUser() {
        return olsUser;
    }

    public String getOlsPassword() {
        return olsPassword;
    }
}
