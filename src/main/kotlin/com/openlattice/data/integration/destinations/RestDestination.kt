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

package com.openlattice.data.integration.destinations

import com.openlattice.client.RetrofitFactory
import com.openlattice.data.EntityKey
import com.openlattice.data.integration.*
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class RestDestination( environment: RetrofitFactory.Environment , authToken: String ) : IntegrationDestination {
    override fun integrateEntities(data: Set<Entity>, entityKeyIds: Map<EntityKey, UUID>) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun integrateAssociations(data: Set<Association>, entityKeyIds: Map<EntityKey, UUID>) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun accepts(): StorageDestination {
        return StorageDestination.REST
    }
}