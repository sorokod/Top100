package org.xor.top100

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertTrue
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource


internal class MergeTest {

    companion object {
        @JvmStatic
        fun chunkData() = arrayOf(
            arrayOf(100, 10),
            arrayOf(1_000, 100),
            arrayOf(9_000_000, 1_000_000),
            arrayOf(11_000_000, 1_000_000)
        )
    }

    @ParameterizedTest(name = "partition should write expected values ({0}, {1}, {2})")
    @MethodSource("chunkData")
    fun `partition should write expected values`(count: Long, chunkSize: ElementIntCount) {
        val dataFile = DataGenerator.fixed(1_000, count, DATA_DIR).absolutePath

        MemoryMapped(dataFile).use { mm ->
            val buffers = mm.partition(chunkSize)

            buffers.forEach { buffer ->
                while (buffer.hasRemaining()) {
                    buffer.put(0L)
                }
            }

            val actual = mmToArray(dataFile)
            val expected = Array(count.toInt()) { 0L }
            assertArrayEquals(expected, actual)
        }
    }

    @ParameterizedTest(name = "mergeBuffers should result in sorted output({0}, {1})")
    @MethodSource("chunkData")
    fun `mergeBuffers should result in sorted output`(count: Long, chunkSize: ElementIntCount) {
        val dataFile = DataGenerator.random(1_000, count, DATA_DIR).absolutePath

        MemoryMapped(dataFile).use { mm ->
            sortChunks(mm, chunkSize)

            val buffers = mm.partition(chunkSize)

            val outFile = "${dataFile}_OUT"
            mergeBuffers(buffers, file2Dos(outFile))

            assertTrue(mmIsSorted(outFile))
        }
    }


    @ParameterizedTest(name = "sortAndMerge should sort({0}, {1})")
    @MethodSource("chunkData")
    fun `sortAndMerge`(count: Long, chunkSize: ElementIntCount) {
        val dataFile = DataGenerator.random(1000, count, DATA_DIR).absolutePath

        sortAndMerge(dataFile, chunkSize)

        val list = mmToList("${dataFile}_OUT")
        assertEquals(count, list.size.toLong(), "Unexpected output size")
        assertTrue(mmIsSorted("${dataFile}_OUT"))
    }
}