package kr.djgis.shpbackup3

import kr.djgis.shpbackup3.network.PostgresConn
import kr.djgis.shpbackup3.property.Config
import kr.djgis.shpbackup3.property.at
import kr.djgis.shpbackup3.property.initPropertyAt
import org.postgresql.util.PSQLException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileNotFoundException

object Execute {

    // TODO: Apache CommonIO + 백업 주기 조절
    // TODO: 모든 작업 정상 종료 후 후실행.txt 명령 추가
    private var shpFiles: Array<File>? = null
    private val tableCodes = ArrayList<String>()
    private var queries = arrayOf<String>()
    val logger: Logger = LoggerFactory.getLogger(Execute.javaClass)

    fun run() = try {
        setupProperties()
        setupTables()
        setupStatement()
    } catch (e: Exception) {
        logger.error(e.message)
    }

    @Throws(Exception::class)
    private fun setupProperties() {
        shpFiles = File(Config.filePath).listFiles { file ->
            file.name.toLowerCase().endsWith("shp")
        }
        if (shpFiles.isNullOrEmpty()) {
            throw FileNotFoundException(".shp File Not Found")
        }
    }

    private fun setupTables() {
        val tableList = initPropertyAt("./table.properties")
        shpFiles?.forEach { file ->
            tableCodes.add(file.name.replace(".shp", "") at tableList)
        }
    }

    @Throws(Throwable::class)
    private fun setupStatement() {
//        val mysqlConn = MysqlConn.getConnection()
        val postgresConn = PostgresConn.getConnection()
        postgresConn.open(rollback = true) { connection ->
            connection.createStatement().use { statement ->
                tableCodes.forEach { tableCode ->

                    queries = arrayOf(
                        "INSERT INTO $tableCode(\"ftr_idn\", \"data\") VALUES(422, '테스트용 데이터')",
                        "INSERT INTO $tableCode(\"ftr_idn\", \"data\") VALUES('테스트용 데이터')", // 잘못된 데이터
                        "INSERT INTO $tableCode(\"ftr_idn\", \"data\") VALUES(45, '테스트용 데이터')",
                        "INSERT INTO $tableCode(\"ftr_idn\", \"data\") VALUES(52, '테스트용 데이터')",
                        "INSERT INTO $tableCode(\"ftr_idn\", \"data\") VALUES(111, '테스트용 데이터')",
                        "INSERT INTO $tableCode(\"ftr_idn\", \"data\") VALUES(52, '테스트 대체 데이터')", // 중복된 ID
                        "INSERT INTO $tableCode(\"ftr_idn\", \"data\") VALUES(764, '테스트용 데이터')"
                    )
                    statement.execute("TRUNCATE TABLE $tableCode")
//                    statement.execute("SELECT SETVAL('public.${tableCode}_id_seq',1,false)")
                    val length = queries.size
                    for (i in 0 until length) try {
                        statement.execute(queries[i])
                    } catch (e: PSQLException) {
                        logger.error("($tableCode) ${e.message}")
                        logger.debug(queries[i])
                        continue
                    } finally {
                        connection.commit()
                    }
                }
            }
        }
    }
}
