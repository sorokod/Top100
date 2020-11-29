package org.xor.top100

import java.io.DataOutputStream
import java.nio.LongBuffer
import java.nio.channels.FileChannel
import kotlin.time.measureTime


//fun splitSortMerge(file: String, bufferCapacity: LongCount) {
//
//    measureTime {
//        val inChannel = file2channel(file)
//
//        sort(inChannel, bufferCapacity)
//
//        val buffers = createBuffers(inChannel, bufferCapacity)
////        log("[splitSortMerge] created ${buffers.size} buffers")
////
////        buffers.forEachIndexed { i, buffer ->
////            measureTime { sortBuffer(buffer) }.also { duration -> log("[splitSortMerge] sorted $i in: $duration") }
////        }
////        log("[splitSortMerge] sorted ${buffers.size} buffers")
//
////        mergeBuffers(buffers, inChannel.size(), file2channel("${file}_OUT"))
//
//
////        mergeBuffers(buffers, inChannel.size(), file2Dos("${file}_OUT"))
//
//        mergeBuffers(buffers, file2Dos("${file}_OUT"))
//    }.also { duration -> log("[splitSortMerge] DONE in: $duration") }
//}


fun mergeBuffers(buffers: Array<LongBuffer>, dos: DataOutputStream) {
    dos.use { dos ->
        var consumedSoFar: LongCount = 0
        var mark = System.currentTimeMillis()

        val bufferList = buffers.toList()
        val almso = AlwaysSorted(bufferList)

        log("[mergeBuffers] starting with  ${bufferList.size} buffers")

        almso.forEach { value ->
            dos.writeLong(value)

            consumedSoFar++
            if ((consumedSoFar % 1_000_000) == 0L) {
                log("[mergeBuffers] completed ${consumedSoFar / 1_000_000} in ${System.currentTimeMillis() - mark}")
                mark = System.currentTimeMillis()
            }
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