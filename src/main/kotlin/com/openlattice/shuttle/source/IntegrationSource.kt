package com.openlattice.shuttle.source

import java.io.InputStream

abstract class IntegrationSource : Sequence<InputStream> {
    abstract override fun iterator(): Iterator<InputStream>
}
