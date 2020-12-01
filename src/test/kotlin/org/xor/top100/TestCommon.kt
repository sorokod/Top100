package org.xor.top100

import org.junit.Assert
import java.io.File
import java.io.RandomAccessFile
import java.nio.channels.FileChannel

const val DATA_DIR = "src/test/resources"


fun mmForEachValue(filePath: String, block: (Long) -> Unit) {
    val fChannel: FileChannel = RandomAccessFile(File(filePath), "r").channel

    fChannel.use { chan ->
        val buffer = chan.map(FileChannel.MapMode.READ_ONLY, 0, chan.size()).asLongBuffer()

        for (i in 0 until buffer.limit()) {
            block(buffer[i])
        }
    }
}

fun mmIsSorted(filePath: String): Boolean {
    val fc: FileChannel = RandomAccessFile(File(filePath), "rw").channel

    fc.use { fc ->
        val buffer = longBuffer(fc, 0, fc.size())

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

fun <T : Comparable<T>> isSorted(data: List<T>): Boolean {
    for (i in 0 until data.size - 1) {
        if (data[i] > data[i + 1]) {
            return false
        }
    }
    return true
}


fun assertSorted(actual: List<Long>) {
    Assert.assertTrue(isSorted(actual))
}


fun mmToList(filePath: String): List<Long> {
    val valueList = ArrayList<Long>()
    mmForEachValue(filePath) { value -> valueList.add(value) }
    return valueList;
}

fun mmToArray(filePath: String): Array<Long> =
    mmToList(filePath).toTypedArray()


val mmPrint = { filePath: String -> mmForEachValue(filePath) { value: Long -> println(value) } }
