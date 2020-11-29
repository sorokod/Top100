package org.xor.top100

import org.junit.Assert
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

    @ParameterizedTest(name = "createBuffers should write expected values ({0}, {1}, {2})")
    @MethodSource("chunkData")
    fun `createBuffers should write expected values`(count: Long, bufferCapacity: LongCount) {
        val dataFile = DataGenerator.fixed(1_000, count, DATA_DIR).absolutePath

        val chan = file2channel(dataFile)
        val buffers = createBuffers(chan, bufferCapacity)

        buffers.forEach() { buffer ->
            while (buffer.hasRemaining()) {
                buffer.put(0L)
            }
        }

        val actual = mmToArray(dataFile)
        val expected = Array(count.toInt()) { 0L }
        Assert.assertArrayEquals(expected, actual)
    }

    @ParameterizedTest(name = "mergeBuffers should result in sorted output({0}, {1})")
    @MethodSource("chunkData")
    fun `mergeBuffers should result in sorted output`(count: Long, bufferCapacity: LongCount) {
        val dataFile = DataGenerator.random(1_000, count, DATA_DIR).absolutePath
        val inChan = file2channel(dataFile)

        sortChunks(inChan, bufferCapacity)
        val buffers = createBuffers(inChan, bufferCapacity)

        val outFile = "${dataFile}_OUT"

        mergeBuffers(buffers, file2Dos(outFile))

        Assert.assertTrue(mmIsSorted(outFile))
    }

//
//    @ParameterizedTest(name = "should be sorted after splitSortMerge({0}, {1}, {2})")
//    @MethodSource("largeData")
//    fun `should be sorted after splitSortMerge`(valueRange: Long, count: Long, bufferCapacity: LongCount) {
//        val dataFile = DataGenerator.random(valueRange, count, dataFileDir).absolutePath
//
//        splitSortMerge(dataFile, bufferCapacity)
//
//        val list = mmToList("${dataFile}_OUT")
//        junit.framework.Assert.assertEquals("Unexpected output size", count, list.size.toLong())
//        Assert.assertTrue(mmIsSorted("${dataFile}_OUT"))
//    }

}