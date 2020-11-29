package org.xor.top100

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode.READ_WRITE
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.RecursiveAction
import java.util.concurrent.TimeUnit
import kotlin.time.measureTime


/**
 * Sorts the individual chunks of the provided FileChannel
 */
fun sortChunks(chan: FileChannel, chunkSize: LongCount) {
    val elementCount: LongCount = chan.size() / LONG_SIZE
    val bufferSize: ByteCount = LONG_SIZE * chunkSize
    val chunkCount: Int = (elementCount / chunkSize).toInt()

    val sortingArray = LongArray(chunkSize.toInt()) { 0L }

    measureTime {
        for (i in 0 until chunkCount) {
            val buffer = chan.map(READ_WRITE, i * bufferSize, bufferSize).asLongBuffer()
            buffer.get(sortingArray)
            sortingArray.sort()
            buffer.rewind()
            buffer.put(sortingArray)
            log("[sort] : $i")
        }
    }.also { duration -> log("[sortChunks] done. elementCount=$elementCount | chunkCount=$chunkCount | duration=$duration") }
}
// ############################################################


class Sorter(val chan: FileChannel, val chunkNumber: Int, val chunkSize: Int) : RecursiveAction() {
    val elementCount: LongCount = chan.size() / LONG_SIZE
    val bufferSize: ByteCount = LONG_SIZE * chunkSize.toLong()
    val chunkCount: Int = (elementCount / chunkSize).toInt()

    override fun compute() {
        if (chunkNumber == -1) {
            invokeAll(subTasks())
        } else {
            sort()
        }
    }

    private fun subTasks(): List<Sorter> {
        val tasks = ArrayList<Sorter>()
        for (i in 0 until chunkCount) {
            tasks.add(Sorter(chan, i, chunkSize))
        }
        log("[Sorter] taskCount=${tasks.size} ")
        return tasks
    }

    private fun sort() {
        val sortingArray = LongArray(chunkSize.toInt()) { 0L }
        val buffer = chan.map(READ_WRITE, chunkNumber * bufferSize.toLong(), bufferSize.toLong()).asLongBuffer()
        buffer.get(sortingArray)
        sortingArray.sort()
        buffer.rewind()
        buffer.put(sortingArray)
        log("[sort] : chunkNumber=$chunkNumber of $chunkCount")
    }
}

fun sortChunks2(chan: FileChannel, chunkSize: Int) {
    val elementCount: LongCount = chan.size() / LONG_SIZE
    val bufferSize: ByteCount = LONG_SIZE * chunkSize.toLong()
    val chunkCount: Int = (elementCount / chunkSize).toInt()

    val es = Executors.newFixedThreadPool(4);

    measureTime {
        for (i in 0 until chunkCount) {
            es.execute(S(chan, i, chunkSize))
        }
        es.shutdown()
        es.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)
    }.also { duration -> log("[sortChunks2] done. elementCount=$elementCount | chunkCount=$chunkCount | duration=$duration") }
}

class S(val chan: FileChannel, val chunkNumber: Int, val chunkSize: Int) : Runnable {
    private val bufferSize: ByteCount = LONG_SIZE * chunkSize.toLong()
    override fun run() {
        val sortingArray = LongArray(chunkSize.toInt()) { 0L }
        val buffer = chan.map(READ_WRITE, chunkNumber * bufferSize.toLong(), bufferSize.toLong()).asLongBuffer()
        buffer.get(sortingArray)
        sortingArray.sort()
        buffer.rewind()
        buffer.put(sortingArray)
        log("[S] : chunkNumber=$chunkNumber")
    }

}

fun sortChunks3(chan: FileChannel, chunkSize: LongCount) {
    val elementCount: LongCount = chan.size() / LONG_SIZE
    val bufferSize: ByteCount = LONG_SIZE * chunkSize
    val chunkCount: Int = (elementCount / chunkSize).toInt()

    val sortingArray = LongArray(chunkSize.toInt()) { 0L }
    val bb = ByteBuffer.allocate(bufferSize.toInt())

    measureTime {
        for (i in 0 until chunkCount) {
            chan.read(bb)
            bb.rewind()
            val ll = bb.asLongBuffer()
            ll.get(sortingArray)
            sortingArray.sort()
            ll.rewind()
            ll.put(sortingArray)
            chan.write(bb)
            log("[sort] : $i")
        }
    }.also { duration -> log("[sortChunks] done. elementCount=$elementCount | chunkCount=$chunkCount | duration=$duration") }
}

