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

package com.openlattice.shuttle;

import com.auth0.client.auth.AuthAPI;
import com.auth0.exception.Auth0Exception;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MissionControl {

    private static final Logger logger = LoggerFactory.getLogger( MissionControl.class );

    private static final String AUTH0_CLIENT_ID     = "o8Y2U2zb5Iwo01jdxMN1W2aiN8PxwVjh";
    private static final String AUTH0_CLIENT_DOMAIN = "openlattice.auth0.com";
    private static final String AUTH0_CONNECTION    = "Username-Password-Authentication";
    private static final String AUTH0_SCOPES        = "openid email nickname roles user_id organizations";

    private static final AuthAPI client = buildClient( AUTH0_CLIENT_ID );


    private static final Stopwatch watch = Stopwatch.createStarted();
    private static final Lock      lock  = new ReentrantLock();


    @VisibleForTesting
    static String getIdToken( AuthAPI auth0, String realm, String username, String password )
            throws Auth0Exception {
        return auth0
                .login( username, password, realm )
                .setScope( AUTH0_SCOPES )
                .setAudience( "https://api.openlattice.com" )
                .execute()
                .getIdToken();
    }

    @VisibleForTesting
    static AuthAPI buildClient( String clientId ) {
        return new AuthAPI( AUTH0_CLIENT_DOMAIN, clientId, "" );
    }

    public static String getIdToken( String username, String password ) throws Auth0Exception {
        return getIdToken( client, AUTH0_CONNECTION, username, password );
    }

    public static void signal() {

        if ( ( watch.elapsed( TimeUnit.MILLISECONDS ) < 250 ) && lock.tryLock() ) {
            watch.reset().start();
            lock.unlock();
        }
    }

    public static void waitForIt() {

        if ( watch.elapsed( TimeUnit.MILLISECONDS ) > 1000 ) {
            return;
        } else {
            try {
                Thread.sleep( 1000 );
            } catch ( InterruptedException e ) {
                logger.error( "Interrupt while waiting on work.", e );
            }
        }
    }
}
