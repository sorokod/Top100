package org.xor.top100

import java.io.DataOutputStream
import java.nio.LongBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode.READ_WRITE
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.TimeUnit.SECONDS

/**
 * Sorts [inFile] by first sorting individual chunks of size [chunkSize] and then merging them
 * into a file of the same name post-fixed  with "_OUT"
 */
fun sortAndMerge(inFile: String, chunkSize: ElementIntCount) {
    val fc = file2channel(inFile)

    sortChunks(fc, chunkSize)

    partition(fc, chunkSize).also { buffers ->  mergeBuffers(buffers, file2Dos("${inFile}_OUT"))}
}

/**
 * Chunks partition the channel, here we sort all the chunks.
 * The changes are reflected in the underlying file
 */
fun sortChunks(fc: FileChannel, chunkSize: ElementIntCount) {
    val elementCount: ElementCount = fc.size() / LONG_SIZE
    val bufferSize: ByteIntCount = LONG_SIZE * chunkSize
    val chunkCount: Int = (elementCount / chunkSize).toInt()

    val sortingArray = LongArray(chunkSize) { 0L }
    val tLogger = TimingLogger(step = 10, SECONDS, "[sort] sorted %d chunks of $chunkCount . Step in: %d sec.")

    for (i in 0 until chunkCount) {
        val buffer = longBuffer(fc, i * bufferSize.toLong(), bufferSize.toLong())

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
            TimingLogger(step = 10_000_000, MILLISECONDS, "[mergeBuffers] merged %d mil. Step in: %d msc.", 1_000_000)

        AlwaysSorted(buffers).forEach { value ->
            dos.writeLong(value)
            tLogger.tick()
        }
    }
}

/**
 * Creates a partition of the FileChannel into buffers
 */
fun partition(fc: FileChannel, chunkSize: Int): Array<LongBuffer> {
    val elementCount: Long = fc.size() / LONG_SIZE
    val numOfBuffers: Int = (elementCount / chunkSize).toInt()
    val bufferSize: ByteIntCount = LONG_SIZE * chunkSize

    return Array(numOfBuffers) { i ->
        longBuffer(fc, i * bufferSize.toLong(), bufferSize.toLong())
    }
}

inline fun longBuffer(fc : FileChannel, position: Long, size: Long) : LongBuffer =
    fc.map(READ_WRITE, position, size).asLongBuffer()
