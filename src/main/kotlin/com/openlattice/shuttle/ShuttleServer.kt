package com.openlattice.shuttle

import com.kryptnostic.rhizome.configuration.websockets.BaseRhizomeServer
import com.kryptnostic.rhizome.core.RhizomeApplicationServer
import com.kryptnostic.rhizome.core.RhizomeApplicationServer.DEFAULT_PODS
import com.kryptnostic.rhizome.pods.AsyncPod
import com.kryptnostic.rhizome.pods.ConfigurationPod
import com.kryptnostic.rhizome.pods.HazelcastPod
import com.kryptnostic.rhizome.pods.hazelcast.RegistryBasedHazelcastInstanceConfigurationPod
import com.openlattice.auditing.pods.AuditingConfigurationPod
import com.openlattice.auth0.Auth0Pod
import com.openlattice.aws.AwsS3Pod
import com.openlattice.datastore.pods.ByteBlobServicePod
import com.openlattice.hazelcast.pods.MapstoresPod
import com.openlattice.hazelcast.pods.SharedStreamSerializersPod
import com.openlattice.jdbc.JdbcPod
import com.openlattice.postgres.PostgresPod
import com.openlattice.shuttle.pods.*
import com.openlattice.shuttle.serializers.ShuttleSharedStreamSerializers
import com.openlattice.tasks.pods.TaskSchedulerPod


private val shuttlePods = arrayOf(
        RegistryBasedHazelcastInstanceConfigurationPod::class.java,
        JdbcPod::class.java,
        AuditingConfigurationPod::class.java,
        AwsS3Pod::class.java,
        ByteBlobServicePod::class.java,
        MapstoresPod::class.java,
        PostgresPod::class.java,
        SharedStreamSerializersPod::class.java,
        ShuttleStreamSerializersPod::class.java,
        TaskSchedulerPod::class.java,
        ShuttleServicesPod::class.java,
        ShuttleServletsPod::class.java,
        ShuttleMapstoresPod::class.java,
        Auth0Pod::class.java,
        ShuttleSecurityPod::class.java,
        MissionParametersPod::class.java
)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class ShuttleServer : BaseRhizomeServer(*shuttlePods + DEFAULT_PODS)