package com.openlattice.shuttle

import com.kryptnostic.rhizome.configuration.websockets.BaseRhizomeServer
import com.kryptnostic.rhizome.pods.hazelcast.RegistryBasedHazelcastInstanceConfigurationPod
import com.openlattice.auditing.pods.AuditingConfigurationPod
import com.openlattice.auth0.Auth0Pod
import com.openlattice.aws.AwsS3Pod
import com.openlattice.datastore.pods.ByteBlobServicePod
import com.openlattice.hazelcast.pods.MapstoresPod
import com.openlattice.hazelcast.pods.SharedStreamSerializersPod
import com.openlattice.jdbc.JdbcPod
import com.openlattice.postgres.PostgresPod
import com.openlattice.shuttle.pods.ShuttleMvcPod
import com.openlattice.shuttle.pods.ShuttleServicesPod
import com.openlattice.shuttle.pods.ShuttleServletsPod
import com.openlattice.tasks.pods.TaskSchedulerPod


private val shuttlePods = arrayOf(
        AuditingConfigurationPod::class.java,
        AwsS3Pod::class.java,
        ByteBlobServicePod::class.java,
        JdbcPod::class.java,
        MapstoresPod::class.java,
        PostgresPod::class.java,
        SharedStreamSerializersPod::class.java,
        TaskSchedulerPod::class.java,
        ShuttleServicesPod::class.java,
        ShuttleServletsPod::class.java,
        ShuttleMvcPod::class.java,
        RegistryBasedHazelcastInstanceConfigurationPod::class.java,
        Auth0Pod::class.java
)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class ShuttleServer : BaseRhizomeServer(*shuttlePods) {

    fun main(args: Array<String>) {
        ShuttleServer().start(*args)
    }

}