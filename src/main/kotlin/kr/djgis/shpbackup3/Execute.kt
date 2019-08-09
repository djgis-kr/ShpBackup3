package kr.djgis.shpbackup3

import kr.djgis.shpbackup3.network.PostgresConn
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileFilter
import java.io.FileNotFoundException

object Execute {

    // TODO: Apache CommonIO + 백업 주기 +
    private var shpFiles: Array<File>? = null
    private val logger: Logger = LoggerFactory.getLogger(Execute.javaClass)

    fun run() = try {
//        setupProperties()
        setupStatement()
    } catch (e: Exception) {
        logger.error(e.message)
    }

    @Throws(Exception::class)
    private fun setupProperties() {
        shpFiles = File(Config.filePath)
            .listFiles(FileFilter { it.name.toLowerCase().endsWith("shp") })
        if (shpFiles.isNullOrEmpty()) {
            throw FileNotFoundException(".shp File Not Found")
        }
    }

    @Throws(Throwable::class, Exception::class)
    private fun setupStatement() {
//        val mysqlConn = MysqlConn.getConnection()
        val postgresConn = PostgresConn.getConnection()

        postgresConn.open(true) {
            it.executeUpdate("INSERT INTO wtl_pipe_test(\"ftr_idn\", \"data\") VALUES('테스트용 데이터')")
            it.connection.commit()
        }
    }
}