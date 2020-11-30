package org.xor.top100

import java.io.DataOutputStream
import java.nio.LongBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.TimeUnit.SECONDS
import kotlin.time.measureTime


fun sortMerge(file: String, bufferCapacity: LongCount) {
    val inChannel = file2channel(file)

    sortChunks(inChannel, bufferCapacity)

    val buffers = createBuffers(inChannel, bufferCapacity)
    mergeBuffers(buffers, file2Dos("${file}_OUT"))
}


fun mergeBuffers(buffers: Array<LongBuffer>, dos: DataOutputStream) {
    dos.use { dos ->
        val tLogger =
            TimingLogger(step = 1_000_000, MILLISECONDS, "[mergeBuffers] merged %d mil. Step in: %d msc.", 1_000_000)

        AlwaysSorted(buffers).forEach { value ->
            dos.writeLong(value)
            tLogger.tick()
        }
    }
}


/**
 * Create disjoint buffers tha cover the FileChannel
 */
fun createBuffers(chan: FileChannel, bufferCapacity: LongCount): Array<LongBuffer> {
    val elementCount: Long = chan.size() / LONG_SIZE
    val numOfBuffers: Int = (elementCount / bufferCapacity).toInt()
    val bufferSize: ByteCount = LONG_SIZE * bufferCapacity

    return Array(numOfBuffers) { i ->
        chan.map(FileChannel.MapMode.READ_WRITE, i * bufferSize, bufferSize).asLongBuffer()
    }
}