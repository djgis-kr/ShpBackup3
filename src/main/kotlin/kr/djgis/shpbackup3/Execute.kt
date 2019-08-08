package kr.djgis.shpbackup3

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileFilter
import java.io.FileNotFoundException
import java.util.*

object Execute {

    private val property: Properties = Preferences.get()
    private var userName: String? = null
    private var geoOrigin: String? = null
    private var dbCode: String? = null
    private var shpFiles: Array<File>? = null
    private val logger: Logger = LoggerFactory.getLogger(Execute.javaClass)

    fun run() = try {
        initPreferences()
    } catch (e: Exception) {
        logger.error(e.message)
    }

    @Throws(Exception::class)
    private fun initPreferences() {
        userName = property of "user"
        geoOrigin = property of "geo_origin"
        dbCode = property of "db_code"
        shpFiles = File(property of "shp_path")
            .listFiles(FileFilter { it.name.toLowerCase().endsWith("shp") })
        if (shpFiles.isNullOrEmpty()) {
            throw FileNotFoundException(".shp File Not Found")
        }
    }
}