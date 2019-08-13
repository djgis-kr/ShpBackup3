package kr.djgis.shpbackup3.property

import kr.djgis.shpbackup3.Execute.logger
import java.io.BufferedReader
import java.io.FileInputStream
import java.io.InputStreamReader
import java.util.*

private val properties = Properties()

fun initPropertyAt(filePath: String): Properties {
    try {
        FileInputStream(filePath).use {
            properties.load(BufferedReader(InputStreamReader(it, "MS949")))
        }
    } catch (e: Throwable) {
        logger.error(e.message)
    }
    return properties
}

infix fun Properties.of(key: String): String = this[key.toUpperCase()] as String

infix fun String.at(properties: Properties): String = properties[this.replace(".shp", "")] as String