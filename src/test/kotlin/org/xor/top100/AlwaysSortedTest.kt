package org.xor.top100

import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*
import java.lang.IllegalArgumentException
import java.nio.LongBuffer
import kotlin.random.Random

internal class AlwaysSortedTest {

    @Test
    fun `AlwaysSorted - draining results in sorted data`() {
        val buffers = Array<LongBuffer>(10) { LongBuffer.wrap(LongArray(200) { Random.nextLong() }.sortedArray()) }
        val subject = AlwaysSorted(buffers)

        val list = mutableListOf<Long>()
        subject.forEach { list.add(it) }

        assertTrue(isSorted(list))
    }
}