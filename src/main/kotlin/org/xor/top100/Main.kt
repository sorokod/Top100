package org.xor.top100


const val DATA_DIR = "src/.."


fun oneBillion() {
    recordTiming("oneBillion") {
        recordTiming("generate random") {
            DataGenerator.random(valueRange = 1_000, count = ONE_BILLION, DATA_DIR)
        } // 7.5G in 33 sec.

        recordTiming("sortMerge") {
            sortAndMerge("$DATA_DIR/dat_1000000000", chunkSize = TEN_MILLION)
        } // 160 sec

        recordTiming("topN") {
            val topN = topN(100, "$DATA_DIR/dat_1000000000_OUT")
            log(topN.toString())
        } // 25 sec
    } // 220 sec.
}

fun fourBillion() {
    recordTiming("fourBillion") {
        recordTiming("generate random") {
            DataGenerator.random(valueRange = 1_000, count = FOUR_BILLION, DATA_DIR)
        } // 30G in 135 sec.

        recordTiming("sortMerge") {
            sortAndMerge("$DATA_DIR/dat_4000000000", chunkSize = TEN_MILLION)
        } // 25 min.

        recordTiming("topN") {
            val topN = topN(100, "$DATA_DIR/dat_4000000000_OUT")
            log(topN.toString())
        } // 95 sec
    } // 29 min.
}

fun main() {
    oneBillion() // 220 sec
//    fourBillion()  // 29 min
}
