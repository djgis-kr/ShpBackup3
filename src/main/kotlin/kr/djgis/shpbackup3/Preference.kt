package kr.djgis.shpbackup3

import java.io.FileInputStream
import java.util.*

object Preferences {

    private const val FILE_PATH = "./config.properties"

    fun get(): Properties {
        val properties = Properties()
        try {
            FileInputStream(FILE_PATH).use(properties::load)
        } catch (e: Throwable) {
            e.message
        }
        return properties
    }
}

infix fun Properties.of(key: String): String = this[key.toUpperCase()] as String