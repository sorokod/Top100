package org.xor.top100

import java.io.*
import java.nio.channels.FileChannel
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.NANOSECONDS

typealias ByteCount = Long
typealias LongCount = Long

const val LONG_SIZE = 8
const val ONE_MILLION = 1_000_000L
const val TEN_MILLION = 10 * ONE_MILLION
const val ONE_BILLION = 1 * 1_000 * ONE_MILLION
const val FOUR_BILLION = 4 * 1_000 * ONE_MILLION



inline fun log(msg: String) = println(msg)

fun file2channel(filePath: String): FileChannel =
    File(filePath).let { file -> RandomAccessFile(file, "rw").channel }

fun file2Dos(filePath: String) =
    DataOutputStream(BufferedOutputStream(FileOutputStream(filePath), 1024 * 1024))

fun file2Dos(filePath: File) =
    DataOutputStream(BufferedOutputStream(FileOutputStream(filePath), 1024 * 1024))

fun file2Dis(filePath: String) =
    DataInputStream(BufferedInputStream(FileInputStream(filePath), 1024 * 1024))

class TimingLogger(
    private val step: Long,
    private val timeUnit: TimeUnit,
    private val template: String,
    private val counterScaler: Long = 1,
    private val logger: (String) -> Unit = { log(it) }
) {
    private var tickCounter: Long = 0L
    private var timeMark = System.nanoTime()

    fun tick() {
        tickCounter++
        if ((tickCounter % step) == 0L) {

            val t = timeUnit.convert(System.nanoTime() - timeMark, NANOSECONDS)
            val n = tickCounter / counterScaler

            timeMark = System.nanoTime()
            logger(template.format(n, t))
        }
    }
}