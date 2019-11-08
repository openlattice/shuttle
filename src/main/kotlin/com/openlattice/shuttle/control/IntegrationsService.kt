package com.openlattice.shuttle.control

import com.hazelcast.core.HazelcastInstance
import com.openlattice.hazelcast.HazelcastMap

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class IntegrationsService( hazelcastInstance: HazelcastInstance ) {
    private val integrations = hazelcastInstance.getMap<String, Integration>(HazelcastMap.INTEGRATIONS.name)


}