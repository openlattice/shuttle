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
import com.openlattice.data.*
import com.openlattice.data.integration.*
import com.openlattice.data.integration.Entity
import org.jdbi.v3.core.statement.Update
import java.util.*
import java.util.function.Supplier

/**
 * Writes data using the REST API
 */
class RestDestination(
        private val dataApi: DataApi,
        private val updateType: UpdateType = UpdateType.Merge
) : IntegrationDestination {
    override fun integrateEntities(
            data: Set<Entity>,
            entityKeyIds: Map<EntityKey, UUID>,
            updateTypes: Map<UUID, UpdateType>
    ): Long {
        val entitiesByEntitySet = data
                .groupBy({ it.entitySetId }, { entityKeyIds[it.key]!! to it.details })
                .mapValues { it.value.toMap() }

        return entitiesByEntitySet.entries.parallelStream().mapToLong { (entitySetId, entities) ->
            dataApi.updateEntitiesInEntitySet(entitySetId, entities, updateType).toLong()
        }.sum()
    }

    override fun integrateAssociations(
            data: Set<Association>,
            entityKeyIds: Map<EntityKey, UUID>,
            updateTypes: Map<UUID, UpdateType>
    ): Long {

        val entitiesByEntitySet = data
                .groupBy({ it.key.entitySetId }, { entityKeyIds[it.key]!! to it.details })
                .mapValues { it.value.toMap() }

        val entities = data.map {
            val srcDataKey = EntityDataKey(it.src.entitySetId, entityKeyIds[it.src])
            val dstDataKey = EntityDataKey(it.dst.entitySetId, entityKeyIds[it.dst])
            val edgeDataKey = EntityDataKey(it.key.entitySetId, entityKeyIds[it.key])
            DataEdgeKey(srcDataKey, dstDataKey, edgeDataKey)
        }.toSet()

        return entitiesByEntitySet.map { (entitySetId, entities) ->
            dataApi.updateEntitiesInEntitySet(entitySetId, entities, updateType).toLong()
        }.sum() + dataApi.createAssociations(entities).toLong()
    }

    override fun accepts(): StorageDestination {
        return StorageDestination.REST
    }
}