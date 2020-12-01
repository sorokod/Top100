package org.xor.top100

import java.io.DataOutputStream
import java.io.File
import java.util.concurrent.TimeUnit.MILLISECONDS
import kotlin.random.Random

object DataGenerator {

    fun random(valueRange: Long, count: ElementCount, parentDir: String): File =
        gen(count, parentDir) { Random.nextLong(valueRange) }

    fun fixed(value: Long, count: ElementCount, parentDir: String): File =
        gen(count, parentDir) { value }

    internal fun gen(count: ElementCount, parentDir: String, value: () -> Long): File {
        val datFile: File = createDatFile(parentDir, "$count")
        val dos: DataOutputStream = file2Dos(datFile)

        val tLogger = TimingLogger(
            step = TEN_MILLION, MILLISECONDS, "[generate] %d mil. Step in: %d msc.", scaling = ONE_MILLION
        )

        dos.use { dos ->
            var counter: ElementCount = 0

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