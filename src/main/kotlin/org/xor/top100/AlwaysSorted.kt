package org.xor.top100

import java.nio.LongBuffer
import java.util.*


class Box(var value: Long, val ref: LongBuffer) : Comparable<Long> {
    override fun compareTo(other: Long): Int = value.compareTo(other)
    override fun toString(): String = "[Box value=$value | ref=$ref]"
}

class AlwaysSorted(private val buffers: List<LongBuffer>) : Iterator<Long> {

    private val sortedData: MutableList<Box> = List(buffers.size) { i -> Box(buffers[i].get(), buffers[i]) }
        .sortedBy { it.value }
        .toMutableList()

    override fun hasNext() = sortedData.isNotEmpty()

    override fun next(): Long {
        val resultBox = sortedData.removeAt(0)
        val resultValue = resultBox.value
        refresh(resultBox)
        return resultValue
    }

    private fun refresh(box: Box) {
        if (box.ref.hasRemaining()) {
            box.value = box.ref.get()
            val idx = sortedData.binarySearch { it.compareTo(box.value) }
            if (idx < 0) {
                sortedData.add(-idx - 1, box)
            } else {
                sortedData.add(idx, box)
            }
        }
    }
}

