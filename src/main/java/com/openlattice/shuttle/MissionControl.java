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

import com.auth0.Auth0;
import com.auth0.authentication.AuthenticationAPIClient;
import com.google.common.base.Stopwatch;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class MissionControl {

    private static final Logger logger = LoggerFactory.getLogger( MissionControl.class );

    private static final String AUTH0_CLIENT_ID     = "KvwsETaUxuXVjW2cmz2LbdqXQBdYs6wH";
    private static final String AUTH0_CLIENT_DOMAIN = "loom.auth0.com";
    private static final String AUTH0_CONNECTION    = "Username-Password-Authentication";
    private static final String AUTH0_SCOPES        = "openid email nickname roles user_id organizations";

    private static final Auth0 auth0 = new Auth0( AUTH0_CLIENT_ID, AUTH0_CLIENT_DOMAIN );

    private static final AuthenticationAPIClient client = auth0.newAuthenticationAPIClient();

    private static final SparkSession sparkSession;

    private static final Stopwatch watch = Stopwatch.createStarted();
    private static final Lock      lock  = new ReentrantLock();

    static {
        sparkSession = SparkSession.builder()
                .master( "local[8]" )
                .appName( "test" )
                .getOrCreate();
    }

    public static SparkSession getSparkSession() {
        return sparkSession;
    }

    public static String getIdToken( String username, String password ) {

        return client
                .login( username, password )
                .setScope( AUTH0_SCOPES )
                .setConnection( AUTH0_CONNECTION )
                .execute()
                .getIdToken();
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
