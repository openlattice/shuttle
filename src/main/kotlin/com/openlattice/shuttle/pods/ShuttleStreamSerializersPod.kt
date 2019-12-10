package com.openlattice.shuttle.pods

import com.openlattice.shuttle.serializers.ShuttleSharedStreamSerializers
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.FilterType
import org.springframework.stereotype.Component

@Configuration
@ComponentScan(
        basePackageClasses = [ShuttleSharedStreamSerializers::class],
        includeFilters = [ComponentScan.Filter(
                value = [Component::class],
                type = FilterType.ANNOTATION
        )]
)
class ShuttleStreamSerializersPod {}