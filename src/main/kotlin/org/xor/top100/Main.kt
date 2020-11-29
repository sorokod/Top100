package org.xor.top100

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
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
//    DataGenerator.random(valueRange = 1_000, count = ONE_BILLION, DATA_DIR) // 7.5G in 38 sec.
//    DataGenerator.random(valueRange = 1_000, count = FOUR_BILLION, DATA_DIR) // 30G in 170 sec.

//    sortChunks(file2channel("$DATA_DIR/dat_1000000000"),  1_000_000) // 35 sec
    sortChunks(file2channel("$DATA_DIR/dat_4000000000"),  1_000_000) // 490 sec


}
