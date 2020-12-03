package org.xor.top100

import java.io.DataOutputStream
import java.nio.LongBuffer
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.TimeUnit.SECONDS

/**
 * Sorts [inFile] by first sorting individual chunks of size [chunkSize] and then merging them
 * into a file of the same name post-fixed  with "_OUT"
 */
fun sortAndMerge(inFile: String, chunkSize: ElementIntCount) =
    MemoryMapped(inFile).use { mm ->
        sortChunks(mm, chunkSize)
        mm.partition(chunkSize).also { buffers ->
            mergeBuffers(buffers, file2Dos("${inFile}_OUT"))
        }
    }


/**
 * Chunks partition the channel, here we sort all the chunks.
 * The changes are reflected in the underlying file
 */
fun sortChunks(mm: MemoryMapped, chunkSize: ElementIntCount) {
    val elementCount: ElementCount = mm.size() / LONG_SIZE
    val bufferSize: ByteIntCount = LONG_SIZE * chunkSize
    val chunkCount: Int = (elementCount / chunkSize).toInt()

    val sortingArray = LongArray(chunkSize) { 0L }
    val tLogger = TimingLogger(step = 10, SECONDS, "[sort] sorted %d chunks of $chunkCount . Step in: %d sec.")

    for (i in 0 until chunkCount) {
        val buffer = mm.getBuffer(i * bufferSize.toLong(), bufferSize.toLong())

        buffer.get(sortingArray)
        sortingArray.sort()
        buffer.rewind()
        buffer.put(sortingArray)

        tLogger.tick()
    }
}

fun mergeBuffers(buffers: Array<LongBuffer>, dos: DataOutputStream) {
    dos.use { dos ->
        val tLogger =
            TimingLogger(
                step = 10_000_000,
                MILLISECONDS,
                "[mergeBuffers] merged %d mil. Step in: %d msc.",
                1_000_000
            )

        AlwaysSorted(buffers).forEach { value ->
            dos.writeLong(value)
            tLogger.tick()
        }
    }
}
