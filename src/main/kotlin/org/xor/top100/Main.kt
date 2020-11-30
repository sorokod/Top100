package org.xor.top100

import kotlin.time.measureTime


const val DATA_DIR = "src/.."


fun oneBillion() {
    measureTime {
        measureTime {
            DataGenerator.random(valueRange = 1_000, count = ONE_BILLION, DATA_DIR)
        }.also { log("[generate] DONE in: $it") } // 7.5G in 45 sec.

        measureTime {
            sortMerge("$DATA_DIR/dat_1000000000", bufferCapacity = TEN_MILLION)
        }.also { log("[sortMerge] DONE in: $it") } // 370 sec

        measureTime {
            val topN = topN(100, "$DATA_DIR/dat_1000000000_OUT")
            log(topN.toString())
        }.also { log("[TopN] done. duration=$it") } // 45 sec

    }.also { log("[oneBillion] done. duration=$it") } // 480 sec.
}

fun fourBillion() {
    DataGenerator.random(valueRange = 1_000, count = FOUR_BILLION, DATA_DIR) // 30G in 170 sec.

//    sortChunks(file2channel("$DATA_DIR/dat_4000000000"),  1_000_000) // 500 sec
    sortMerge("$DATA_DIR/dat_4000000000", 10_000_000) // 30 min

}

fun main() {
    oneBillion()
}
