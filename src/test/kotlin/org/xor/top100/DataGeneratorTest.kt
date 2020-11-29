package org.xor.top100

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.xor.top100.DataGenerator.fixed
import org.xor.top100.DataGenerator.random
import kotlin.Long.Companion.MAX_VALUE

internal class DataGeneratorTest {

    private val dataDir = "src/test/resources"

    companion object {
        @JvmStatic
        fun countData() = arrayOf(
            arrayOf(1_000L),
            arrayOf(5_000_000L)
        )
    }

    @ParameterizedTest(name = "random should generate file of correct length: {0}")
    @MethodSource("countData")
    fun `random should generate file of correct length`(count: LongCount) {
        val expectedLength = LONG_SIZE * count

        random(valueRange = 1_000, count, dataDir).also { file ->
            assertEquals(expectedLength, file.length())
        }
    }

    @ParameterizedTest(name = "fixed should generate file with correct value. length: {0}")
    @MethodSource("countData")
    fun `fixed should generate file with correct value`(count: LongCount) {
        val expectedValue = MAX_VALUE
        val expectedLength = LONG_SIZE * count

        fixed(expectedValue, count, dataDir).also { file ->
            assertEquals(expectedLength, file.length())
            mmForEachValue(file.absolutePath) { assertEquals(expectedValue, it) }
        }
    }
}