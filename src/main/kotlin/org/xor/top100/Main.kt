package org.xor.top100

import java.io.EOFException
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.LongBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.Executors
import java.util.concurrent.ForkJoinPool
import kotlin.random.Random
import kotlin.time.measureTime


const val DATA_DIR = "src/.."
const val FOUR_BILLION = 4 * 1_000 * 1_000_000L
const val ONE_BILLION = 1 * 1_000 * 1_000_000L


fun boost(chan: FileChannel) {
    val bb = ByteBuffer.allocate(1_000_000)

    var ctr = 0
    measureTime {
        while (true) {
            try {
                chan.read(bb)
                ctr += bb[0]
            } catch (e: Exception) {
                break
            }
        }
    }.also { duration -> log("[boost] DONE in: $duration ctr=$ctr") }
}


fun main() {
    DataGenerator.random(valueRange = 1_000, count = ONE_BILLION, DATA_DIR) // 7.5G in 30 sec.
//    DataGenerator.random(valueRange = 1_000, count = FOUR_BILLION, DATA_DIR) // 30G in 115 sec.

//    val chan = file2channel("/Users/xor/work/kotlin/top100/dat_1000000000")

//    measureTime {
//        val sorter = Sorter(chan, -1, 1_000_000)
//
////        val parallelism = ForkJoinPool.getCommonPoolParallelism()
////        println("parallelism=$parallelism") // 15
////        val pool = Executors.newWorkStealingPool(8) as ForkJoinPool
//
//        val pool = ForkJoinPool.commonPool()
//        pool.invoke(sorter)
//        pool.shutdown()
//    }.also { duration -> log("[sortPar] DONE in: $duration") }

    sortChunks2(file2channel("/Users/xor/work/kotlin/top100/dat_1000000000"),  1_000_000)
//    sortChunks(file2channel("/Users/xor/work/kotlin/top100/dat_1000000000"),  1_000_000)
//    sortChunks3(file2channel("/Users/xor/work/kotlin/top100/dat_1000000000"),  1_000_000)


}
