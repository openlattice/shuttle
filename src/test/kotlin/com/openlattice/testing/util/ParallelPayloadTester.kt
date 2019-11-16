/*
 * Copyright (C) 2019. OpenLattice, Inc.
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

package com.openlattice.testing.util

import com.dataloom.streams.StreamUtil
import com.google.common.collect.Queues
import com.google.common.util.concurrent.MoreExecutors
import org.apache.commons.lang3.RandomUtils
import org.junit.Ignore
import org.junit.Test
import java.util.concurrent.Semaphore
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.streams.asStream

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class ParallelPayloadTester {
    @Test
    @Ignore
    fun testPayload() {
        var order = mutableListOf<Long>()
        Payload(1000).asSequence().asStream().parallel()
                .map {
                    println("Start ${Thread.currentThread().id}")
                    Thread.sleep(RandomUtils.nextLong(1, 1500))
                    it
                }
                .map {
                    println("Thread ${Thread.currentThread().id} = $it")
                    Thread.sleep(RandomUtils.nextLong(1, 1500))
                    it ?: -1
                }.forEach {
                    order.add(it)
                }
        println(order)
    }

    @Test
    @Ignore
    fun testPayload2() {
        val order = mutableListOf<Long>()
        (1L..1000L).toList().stream().parallel()
                .map {
                    println("Start ${Thread.currentThread().id}")
                    Thread.sleep(RandomUtils.nextLong(1, 1500))
                    it
                }
                .map {
                    println("Thread ${Thread.currentThread().id} = $it")
                    Thread.sleep(RandomUtils.nextLong(1, 1500))
                    it ?: -1
                }.forEach {
                    order.add(it)
                }
        println(order)
    }

    @Test
    @Ignore
    fun testPayload3() {
        val threads = Runtime.getRuntime().availableProcessors()
        val s = Semaphore(100)
        val executor = MoreExecutors.listeningDecorator(
                ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS, Queues.newArrayBlockingQueue(1000))
        )

        val order = mutableListOf<Long>()
        Payload(Long.MAX_VALUE)
                .asSequence()
                .chunked(10)
                .asStream().parallel()
                .map { p ->
                    //                    s.acquire()
//                    executor.submit<Long> {

                    println("Start ${Thread.currentThread().id}")
                    Thread.sleep(RandomUtils.nextLong(1, 1500))
                    p.max()


//                    }
                }
                .map {
                    //                    s.release()
//                    s.acquire()
//                    executor.submit<Long> {
                    val m = it//.get()
                    println("Thread ${Thread.currentThread().id} = $m")
                    Thread.sleep(RandomUtils.nextLong(1, 1500))
                    m ?: -1L

//                    }
                }.map {
                    //                    executor.submit {

                    order.add(it)//.get())
                    it

//                    }
                }
                .forEach { it }
        println(order)
    }


    @Test
    @Ignore
    fun testPayload4() {
        val threads = Runtime.getRuntime().availableProcessors()
        val s = Semaphore(500)
        val executor = MoreExecutors.listeningDecorator(
                ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS, Queues.newArrayBlockingQueue(1000))
        )

        val order = mutableListOf<Long>()
        val seq = Payload(1000)
                .asSequence()
                .chunked(10)
                .forEach { p ->
                    s.acquire()
                    executor.submit {

                        println("Start ${Thread.currentThread().id}")
                        Thread.sleep(RandomUtils.nextLong(1, 1500))
                        val m = p.max()
                        order.add(m ?: -1)
                        println("Thread ${Thread.currentThread().id} = $m")

                    }.addListener(Runnable { s.release() }, executor)
                }
        s.acquire(500);
        println(order)
    }


    @Test
    @Ignore
    fun testPayload5() {
        var order = mutableListOf<Long>()
        StreamUtil.stream(Payload(1000).asSequence().chunked(10).asIterable())
                .parallel()
                .map { p ->
                    println("Start ${Thread.currentThread().id}")
                    Thread.sleep(1500L - (p.max() ?: 0))
                    p.max()
                }
                .map { m ->
                    println("Thread ${Thread.currentThread().id} = $m")
                    Thread.sleep(RandomUtils.nextLong(1, 1500))
                    m ?: -1
                }
                .map {
                    order.add(it)
                    it
                }
                .forEach { it }
        println(order)
    }
}

class Payload(val limit: Long) : Iterable<Long> {
    override fun iterator(): Iterator<Long> {
        return object : Iterator<Long> {
            override fun next(): Long {
                return ++i
            }

            override fun hasNext(): Boolean {
                return i < limit
            }

            var i = 0L
        }
    }
}