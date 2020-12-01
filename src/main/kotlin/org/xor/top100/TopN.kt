package org.xor.top100

import java.io.EOFException
import java.util.concurrent.TimeUnit.MILLISECONDS


fun topN(n: Int, sortedFile: String): Set<Long> {
    val countMap = mutableMapOf<Long, Long>()

    fun addOrReplace(value: Long, count: Long) {
        if (countMap.size < n) {
            countMap[value] = count
        } else {
            val minEntry = countMap.entries.minByOrNull { it.value }!!
            if (minEntry.value < count) {
                countMap.remove(minEntry.key)
                countMap[value] = count
            }
        }
    }


    val tLogger = TimingLogger(step = TEN_MILLION, MILLISECONDS, "[topN] %d mil. Step in: %d msc.", ONE_MILLION)

    val dis = file2Dis(sortedFile)
    var value = 0L
    var count = 0L
    var done = false

    dis.use { dis ->
        dis.mark(LONG_SIZE)
        value = dis.readLong().also { dis.reset() }
        while (!done) {
            try {
                val current = dis.readLong()
                if (current == value) {
                    count++
                } else {
                    addOrReplace(value, count)
                    value = current
                    count = 1
                }
                tLogger.tick()
            } catch (ex: EOFException) {
                done = true
            }
        }
        addOrReplace(value, count)
        return countMap.keys
    }
}
