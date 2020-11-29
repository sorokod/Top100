package org.xor.top100

import java.io.*
import java.nio.channels.FileChannel

typealias ByteCount = Long
typealias LongCount = Long

const val LONG_SIZE = 8


inline fun log(msg: String) = println(msg)

fun file2channel(filePath: String): FileChannel =
    File(filePath).let { file -> RandomAccessFile(file, "rw").channel }

fun file2Dos(filePath: String) =
    DataOutputStream(BufferedOutputStream(FileOutputStream(filePath), 1024 * 1024))

fun file2Dos(file: File) =
    DataOutputStream(BufferedOutputStream(FileOutputStream(file), 1024 * 1024))

