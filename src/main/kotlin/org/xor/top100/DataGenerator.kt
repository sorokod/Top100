package org.xor.top100

import java.io.DataOutputStream
import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MILLISECONDS
import kotlin.random.Random
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

object DataGenerator {

    fun random(valueRange: Long, count: LongCount, parentDir: String): File =
        gen(count, parentDir) { Random.nextLong(valueRange) }

    fun fixed(value: Long, count: LongCount, parentDir: String): File =
        gen(count, parentDir) { value }

    internal fun gen(count: LongCount, parentDir: String, value: () -> Long): File {
        val datFile: File = createDatFile(parentDir, "$count")
        val dos: DataOutputStream = file2Dos(datFile)

        val tLogger = TimingLogger(
            step = TEN_MILLION, MILLISECONDS, "[generate] %d. of $count Step in: %d msc.", ONE_MILLION
        )

        dos.use { dos ->
            var counter: LongCount = 0

            while (counter < count) {
                dos.writeLong(value())
                counter++
                tLogger.tick()
            }
        }
        return datFile
    }

    private fun createDatFile(parentDir: String, postFix: String): File =
        File("$parentDir/dat_$postFix").also {
            it.delete()
            log("[generate] created: ${it.absolutePath}")
        }
}

fun main() {
    DataGenerator.fixed(Long.MAX_VALUE, count = 1_000L, parentDir = "src/main/resources")
}