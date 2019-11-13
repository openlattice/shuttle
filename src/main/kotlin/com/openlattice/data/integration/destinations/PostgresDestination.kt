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

import com.hazelcast.core.HazelcastInstance
import com.openlattice.data.DataGraphManager
import com.openlattice.data.DataGraphService
import com.openlattice.data.EntityKey
import com.openlattice.data.UpdateType
import com.openlattice.data.integration.*
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.datastore.services.EntitySetService
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.Graph
import com.openlattice.graph.core.GraphService
import com.zaxxer.hikari.HikariDataSource
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresDestination(
        private val propertyTypes: Map<UUID, PropertyType>,
        hazelcastInstance: HazelcastInstance,
        hds: HikariDataSource, byteBlobDataManager: ByteBlobDataManager,
        partitionManager: PartitionManager = PartitionManager(hazelcastInstance, hds)
) : IntegrationDestination {
    private val pgedqs = PostgresEntityDataQueryService(hds, byteBlobDataManager, partitionManager)

    private val graphService = Graph(hds, entitySetsManager, partitionManager)
    private val graphManager = DataGraphService()
    override fun integrateEntities(
            data: Set<Entity>, entityKeyIds: Map<EntityKey, UUID>, updateTypes: Map<UUID, UpdateType>
    ): Long {
        data
                .groupBy({ it.entitySetId }, { entityKeyIds.getValue(it.key) to it.details })
                .forEach { (entitySetId, entities) ->
                    pgedqs.upsertEntities(entitySetId, entities.toMap(), propertyTypes)
                }

    }

    override fun integrateAssociations(
            data: Set<Association>, entityKeyIds: Map<EntityKey, UUID>, updateTypes: Map<UUID, UpdateType>
    ): Long {
        data
                .groupBy({ it.entitySetId }, { entityKeyIds.getValue(it.key) to it.details })
                .forEach { (entitySetId, entities) ->
                    pgedqs.upsertEntities(entitySetId, entities.toMap(), propertyTypes)
                }
    }

    override fun accepts(): StorageDestination {
        return StorageDestination.POSTGRES
    }

}