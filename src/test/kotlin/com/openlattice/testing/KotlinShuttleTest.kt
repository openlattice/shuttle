package com.openlattice.testing

import com.google.common.base.Stopwatch
import com.google.common.collect.Queues
import com.google.common.io.Resources
import com.openlattice.data.integration.AddressedDataHolder
import com.openlattice.data.integration.StorageDestination
import com.openlattice.shuttle.MissionControl
import com.openlattice.shuttle.payload.SimplePayload
import com.openlattice.shuttle.test.ShuttleTest.getFlight
import io.findify.s3mock.S3Mock
import org.junit.Assert
import org.junit.Test
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.stream.Stream
import kotlin.streams.asSequence

class KotlinShuttleTest {

    private val uploadingExecutor = Executors.newSingleThreadExecutor()
    private val url = Resources.getResource( "cyphers.csv" )
    private var payload = SimplePayload( url.path )

    init {
        val api = S3Mock.create(8001, "/tmp/s3");
        api.start();
    }

    @Test( timeout = 5000 )
    fun testFoo () {

        val flight = getFlight();

        Assert.assertEquals(1, 1)

        val integratedEntities = mutableMapOf<StorageDestination, AtomicLong>().withDefault { AtomicLong(0L) }
        val integratedEdges = mutableMapOf<StorageDestination, AtomicLong>().withDefault { AtomicLong(0L) }
        val integrationQueue = Queues
                .newArrayBlockingQueue<List<Map<String, Any>>>(
                        Math.max(2, 2 * (Runtime.getRuntime().availableProcessors() - 2))
                )
        val rows = AtomicLong(0)
        val sw = Stopwatch.createStarted()
        val remaining = AtomicLong(0)

        uploadingExecutor.execute {
            Stream.generate { integrationQueue.take() }.parallel()
                    .map { batch ->
                        println("mapping batch")
                        try {
                            rows.addAndGet(batch.size.toLong())
                            return@map impulse()
                        } catch (ex: Exception) {
                            println("caught splosion")
                            remaining.decrementAndGet()
                            return@map AddressedDataHolder(mutableMapOf(), mutableMapOf())
                        }
                    }
                    .forEach { (entities, associations) ->
                        println("About to try hard")
                        try {
                            println("Processed $rows rows.")
                        } catch (ex: Exception) {
                            println("caught ex")
                            MissionControl.fail(1, flight, ex)
                        } catch (err: OutOfMemoryError) {
                            println("caught oom")
                            MissionControl.fail(1, flight, err)
                        } finally {
                            println("finally dec")
                            remaining.decrementAndGet()
                        }
                    }
        }

        payload.payload.asSequence()
                .chunked(1)
                .forEach {
                    println("queueing")
                    remaining.incrementAndGet()
                    integrationQueue.put(it)
                }

        //Wait on upload thread to finish emptying queue.
        while (remaining.get() > 0) {
            println("Waiting on upload to finish...")
            println("Current time spent: ${sw.elapsed(TimeUnit.MILLISECONDS)} ms for flight ${flight.name}")
            Thread.sleep(1000)
        }

        println("Integrated in ${sw.elapsed(TimeUnit.MILLISECONDS)} ms for flight ${flight.name}")
    }

    fun impulse() : AddressedDataHolder {
        println("Impulsing")
        val rand = (1..3).random()
        if ( rand == 2) {
            throw NoSuchElementException()
        }
        return AddressedDataHolder(mutableMapOf(), mutableMapOf())
    }
}
