package org.xor.top100

import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.xor.top100.DataGenerator.gen
import java.io.File
import kotlin.test.assertEquals

/**
 *  Generates data of size = [size] such that values 0..[frequentValues]-1 have frequency [freq] are the most frequent
 */
fun generateFrequentData(frequentValues: Long, freq: Int, size: Int): File {

    require(freq > 1) { "freq must be > 1" }
    require(frequentValues * freq <= size) {
        "Can not generate data of size $size with ${frequentValues * freq} most frequent elements"
    }

    val dataList = mutableListOf<Long>()

    (0 until frequentValues).forEach { i ->
        repeat(freq) {
            dataList.add(i)
        }
    }
    for (i in (0 until size - frequentValues * freq)) {
        dataList.add(frequentValues + i)
    }

    dataList.shuffle()

    var i = 0
    return gen(size.toLong(), DATA_DIR) { dataList[i++] }
}


class TopNTest {

    companion object {
        @JvmStatic
        fun chunkData() = arrayOf(
            arrayOf(10, 100, generateFrequentData(frequentValues = 10, freq = 5, 100).absolutePath),
            arrayOf(17, 100, generateFrequentData(frequentValues = 17, freq = 5, 100).absolutePath),
            arrayOf(21, 1_000, generateFrequentData(frequentValues = 21, freq = 11, 1_000).absolutePath)
        )
    }

    @ParameterizedTest(name = "topN should return top N values({0}, {1} {2})")
    @MethodSource("chunkData")
    fun `topN should return top N values`(frequentValues: Int, size: Int, file: String) {

        sortAndMerge(file, chunkSize = 10)

        val expected = List(frequentValues) { it.toLong() }.toSet()
        assertEquals(expected, topN(frequentValues, "${file}_OUT"))
    }
}