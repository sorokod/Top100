package org.xor.top100

import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode.READ_WRITE
import java.util.concurrent.TimeUnit.SECONDS


/**
 * Sorts the individual chunks of the provided FileChannel
 */
fun sortChunks(chan: FileChannel, chunkSize: LongCount) {
    val elementCount: LongCount = chan.size() / LONG_SIZE
    val bufferSize: ByteCount = LONG_SIZE * chunkSize
    val chunkCount: Int = (elementCount / chunkSize).toInt()

    val sortingArray = LongArray(chunkSize.toInt()) { 0L }
    val tLogger = TimingLogger(step = 10, SECONDS, "[sort] sorted %d chunks of $chunkCount . Step in: %d sec.")

    for (i in 0 until chunkCount) {
        val buffer = chan.map(READ_WRITE, i * bufferSize, bufferSize).asLongBuffer()
        buffer.get(sortingArray)
        sortingArray.sort()
        buffer.rewind()
        buffer.put(sortingArray)

        tLogger.tick()
    }
}

