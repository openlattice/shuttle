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

package com.openlattice.shuttle;

import com.auth0.client.auth.AuthAPI;
import com.auth0.exception.Auth0Exception;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class MissionControlTests {
    private static final String AUTH0_CLIENT_ID  = "KTzgyxs6KBcJHB872eSMe2cpTHzhxS99";
    private static final String AUTH0_CONNECTION = "Tests";

    @Test
    public void testAuth2() throws Auth0Exception {
            String token = MissionControl.getIdToken( "ncric@openlattice.com","QY8Tp[L&ZnJ949" );
    }

    @Test
    public void testAuth() throws Auth0Exception {

        AuthAPI api = MissionControl.buildClient( AUTH0_CLIENT_ID );
        String idToken = MissionControl.getIdToken( api, AUTH0_CONNECTION, "tests@openlattice.com", "openlattice" );
        Assert.assertTrue( "Id token cannot be blank", StringUtils.isNotBlank( idToken ) );
    }
}
