package org.xor.top100

import java.nio.LongBuffer
import java.nio.channels.FileChannel

fun createBuffers(chan: FileChannel, bufferCapacity: LongCount): Array<LongBuffer> {
    val elementCount: Long = chan.size() / LONG_SIZE
    val numOfBuffers: Int = (elementCount / bufferCapacity).toInt()
    val bufferSize: ByteCount = LONG_SIZE * bufferCapacity

    return Array(numOfBuffers) { i ->
        chan.map(FileChannel.MapMode.READ_WRITE, i * bufferSize, bufferSize).asLongBuffer()
    }
}