package com.openlattice.shuttle

import com.kryptnostic.rhizome.configuration.websockets.BaseRhizomeServer
import com.kryptnostic.rhizome.core.RhizomeApplicationServer.DEFAULT_PODS
import com.kryptnostic.rhizome.pods.hazelcast.RegistryBasedHazelcastInstanceConfigurationPod
import com.openlattice.auditing.pods.AuditingConfigurationPod
import com.openlattice.auth0.Auth0Pod
import com.openlattice.aws.AwsS3Pod
import com.openlattice.datastore.pods.ByteBlobServicePod
import com.openlattice.hazelcast.pods.HazelcastQueuePod
import com.openlattice.hazelcast.pods.MapstoresPod
import com.openlattice.hazelcast.pods.NearCachesPod
import com.openlattice.hazelcast.pods.SharedStreamSerializersPod
import com.openlattice.jdbc.JdbcPod
import com.openlattice.postgres.PostgresPod
import com.openlattice.shuttle.pods.*
import com.openlattice.tasks.pods.TaskSchedulerPod


private val shuttlePods = arrayOf(
        //Rhizome pods
        RegistryBasedHazelcastInstanceConfigurationPod::class.java,
        Auth0Pod::class.java,

        //Web pods
        ShuttleServletsPod::class.java,
        ShuttleSecurityPod::class.java,

        //Shuttle pods
        AwsS3Pod::class.java,
        AuditingConfigurationPod::class.java,
        BlackboxPod::class.java,
        ByteBlobServicePod::class.java,
        JdbcPod::class.java,
        HazelcastQueuePod::class.java,
        MapstoresPod::class.java,
        MissionParametersPod::class.java,
        PostgresPod::class.java,
        SharedStreamSerializersPod::class.java,
        ShuttleServicesPod::class.java,
        TaskSchedulerPod::class.java,
        NearCachesPod::class.java,

        //This pod goes last for things to not break
        ShuttleMapstoresPod::class.java
)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class ShuttleServer : BaseRhizomeServer(*shuttlePods + DEFAULT_PODS)