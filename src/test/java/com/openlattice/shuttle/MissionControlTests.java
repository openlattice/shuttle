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

import com.auth0.exception.Auth0Exception;
import com.openlattice.auth0.Auth0Delegate;
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
    public void testAuth() throws Auth0Exception {

        Auth0Delegate auth0 = Auth0Delegate.fromConstants( MissionControl.AUTH0_CLIENT_DOMAIN, AUTH0_CLIENT_ID,
                AUTH0_CONNECTION, MissionControl.AUTH0_SCOPES );
        String idToken = auth0.getIdToken( AUTH0_CONNECTION, "tests@openlattice.com", "openlattice" );
        Assert.assertTrue( "Id token cannot be blank", StringUtils.isNotBlank( idToken ) );
    }
}
