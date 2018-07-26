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

package com.openlattice.shuttle.test;

import com.openlattice.data.DataApi;
import com.openlattice.data.DataIntegrationApi;
import org.mockito.stubbing.Answer;

import java.io.Serializable;

public class Answers implements Serializable {

    private static final long serialVersionUID = 6728584404927356350L;

    private static int createDataInvocationCount    = 0;
    private static int createDataApiInvocationCount = 0;

    public static Answer<Void> incrementCreateDataInvocationCount() {
        return invocation -> {
            createDataInvocationCount++;
            return null;
        };
    }

    public static Answer<DataIntegrationApi> incrementCreateDataIntegrationApiCount( final DataIntegrationApi api ) {
        return invocation -> {
            createDataApiInvocationCount++;
            return api;
        };
    }

    public static int getCreateDataInvocationCount() {
        return createDataInvocationCount;
    }

    public static int getCreateDataIntegrationApiInvocationCount() {
        return createDataApiInvocationCount;
    }

}
