package org.xor.top100

import org.junit.Assert.assertTrue

const val DATA_DIR = "src/test/resources"


fun mmForEachValue(filePath: String, block: (Long) -> Unit) =
    MemoryMapped(filePath).use { mm ->
        val buffer = mm.getBuffer()
        for (i in 0 until buffer.limit()) {
            block(buffer[i])
        }
    }


fun mmIsSorted(filePath: String): Boolean {
    MemoryMapped(filePath).use { mm ->
        val buffer = mm.getBuffer()
        var previuos = Long.MIN_VALUE

        for (i in 0 until buffer.limit()) {
            val current = buffer[i]
            if (current < previuos) {
                println("[$i] prev:$previuos current:$current")
                return false
            } else {
                previuos = current
            }
        }
        return true
    }
}

private fun <T : Comparable<T>> isSorted(data: List<T>): Boolean {
    for (i in 0 until data.size - 1) {
        if (data[i] > data[i + 1]) {
            return false
        }
    }
    return true
}

fun assertSorted(actual: List<Long>) = assertTrue(isSorted(actual))

fun mmToList(filePath: String): List<Long> {
    val valueList = ArrayList<Long>()
    mmForEachValue(filePath) { value -> valueList.add(value) }
    return valueList;
}

fun mmToArray(filePath: String): Array<Long> =
    mmToList(filePath).toTypedArray()
