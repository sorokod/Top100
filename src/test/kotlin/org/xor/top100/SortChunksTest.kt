package org.xor.top100

import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.util.concurrent.ForkJoinPool

class SortChunksTest {

    companion object {
        @JvmStatic
        fun chunkData() = arrayOf(
            arrayOf(100, 10),
            arrayOf(1_000, 100),
            arrayOf(9_000_000, 1_000_000),
            arrayOf(11_000_000, 1_000_000)
        )
    }


    @ParameterizedTest(name = "sortChunks should result in sorted chunks({0}, {1})")
    @MethodSource("chunkData")
    fun `sortChunks is sorting`(count: LongCount, chunkSize: LongCount) {
        val dataFile = DataGenerator.random(1_000, count, DATA_DIR).absolutePath
        val chan = file2channel(dataFile)

        sortChunks(chan, chunkSize)

        mmToList(dataFile).windowed(chunkSize.toInt(), chunkSize.toInt())
            .forEach { chunk -> assertSorted(chunk) }
    }
}