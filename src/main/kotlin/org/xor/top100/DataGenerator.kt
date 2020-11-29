package org.xor.top100

import java.io.DataOutputStream
import java.io.File
import kotlin.random.Random
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

object DataGenerator {

    fun random(valueRange: Long, count: LongCount, parentDir: String): File =
        gen(count, parentDir) { Random.nextLong(valueRange) }

    fun fixed(value: Long, count: LongCount, parentDir: String): File =
        gen(count, parentDir) { value }

    private fun gen(count: LongCount, parentDir: String, value: () -> Long): File {
        val datFile: File = createDatFile(parentDir, "$count")
        val dos: DataOutputStream = file2Dos(datFile)

        measureTime {
            dos.use { dos ->
                var counter: LongCount = 0

                while (counter < count) {
                    dos.writeLong(value())
                    counter++
                }
            }
        }.also { duration -> log("[generate] done. values=$count duration=$duration file=${datFile.absolutePath}") }
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