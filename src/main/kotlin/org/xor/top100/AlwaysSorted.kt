package org.xor.top100

import java.nio.LongBuffer


/**
 * Maintains an always sorted view [sortedData] of the minimal values in the [buffers]
 */
class AlwaysSorted(private val buffers: Array<LongBuffer>) : Iterator<Long> {

    private val sortedData: MutableList<Box> = List(buffers.size) { i -> Box(buffers[i].get(), buffers[i]) }
        .sortedBy { it.value }
        .toMutableList()

    override fun hasNext() = sortedData.isNotEmpty()

    override fun next(): Long = sortedData.first().value.apply { refresh() }

    /**
     * Removes the current min value and inserts the next one in the correct index
     */
    private fun refresh() {
        val box = sortedData.removeAt(0)

        if (box.buffer.hasRemaining()) {
            box.value = box.buffer.get()
            insertIndex(box.value).also { i -> sortedData.add(i, box) }
        }
    }

    private inline fun insertIndex(value: Long): Int {
        val idx = sortedData.binarySearch { it.compareTo(value) }
        return if (idx < 0) {
            -idx - 1
        } else {
            idx
        }
    }
}

/**
 * Holds a [buffer] and its current minimal [value]
 */
private class Box(var value: Long, val buffer: LongBuffer) : Comparable<Long> {
    override fun compareTo(other: Long): Int = value.compareTo(other)
    override fun toString(): String = "[Box value=$value | ref=$buffer]"
}
