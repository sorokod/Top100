package org.xor.top100

import java.io.Closeable
import java.io.File
import java.io.RandomAccessFile
import java.nio.LongBuffer
import java.nio.channels.FileChannel

/**
 * A wrapper of FileChannel
 */
class MemoryMapped(filePath: String) : Closeable {

    private val fc: FileChannel =
        File(filePath).let { file -> RandomAccessFile(file, "rw").channel }

    fun size(): Long = fc.size()

    fun getBuffer(position: Long = 0, size: Long = fc.size()): LongBuffer =
        fc.map(FileChannel.MapMode.READ_WRITE, position, size).asLongBuffer()

    /**
     * Creates a partitioning of the FileChannel into buffers each containing [elementCount] elements
     */
    fun partition(elementCount: Int): Array<LongBuffer> {
        val totalElementCount: Long = fc.size() / LONG_SIZE
        val numOfBuffers: Int = (totalElementCount / elementCount).toInt()
        val bufferSize: ByteIntCount = LONG_SIZE * elementCount

        return Array(numOfBuffers) { i -> getBuffer(i * bufferSize.toLong(), bufferSize.toLong()) }
    }

    override fun close() = fc.close()
}