package kr.djgis.shpbackup3

import kr.djgis.shpbackup3.network.MysqlConnection
import kr.djgis.shpbackup3.network.PostgresConnection
import kr.djgis.shpbackup3.property.Config
import kr.djgis.shpbackup3.property.at
import kr.djgis.shpbackup3.property.initPropertyAt
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.data.simple.SimpleFeatureCollection
import org.opengis.feature.simple.SimpleFeature
import org.postgresql.util.PSQLException
import java.io.BufferedReader
import java.io.File
import java.io.FileNotFoundException
import java.io.FileReader
import java.nio.charset.Charset
import java.sql.ResultSet
import java.util.*
import kotlin.collections.ArrayList

@Deprecated("Deprecated")
object Execute {

    private lateinit var tableList: Properties
    private var shpFiles: Array<File>? = null

    fun run() = try {
        setupProperties()
        setupStatement()
    } catch (e: FileNotFoundException) {
        println(e.message)
        logger.info(e.message)
    } catch (e: Exception) {
        logger.error(e.message)
        e.printStackTrace()
    }

    @Throws(Exception::class)
    private fun setupProperties() {
        tableList = initPropertyAt("./table.properties")
        shpFiles = File(Config.filePath).listFiles { file ->
            file.name.endsWith("shp") && file.length() > 20000L && (file.name at tableList != "")
        }
        if (shpFiles.isNullOrEmpty()) {
            throw FileNotFoundException("${Config.local} 백업 대상인 .shp 파일이 없음 (폴더위치: ${Config.filePath})")
        }
    }

    @Throws(Throwable::class)
    private fun setupStatement() {
        val mConn = MysqlConnection().getConnection()
        val pConn = PostgresConnection().getConnection()
        mConn.createStatement().use { mStmt ->
            pConn.open(true) { postgres ->
                println("${ANSI_GREEN}완료$ANSI_RESET")
                postgres.createStatement().use { pStmt ->
                    val shpFileList: ArrayList<String> = ArrayList()
                    shpFiles?.forEach shpFile@{ file ->
                        shpFileList.add(file.name)
                        print("${Config.local} 다음 백업 중: ${file.name.replace(".shp", "")}")
                        val tableCode = file.name at tableList
                        val featureCollection: SimpleFeatureCollection
                        val store = ShapefileDataStore(file.toURI().toURL())
                        try {
                            store.charset = Charset.forName("MS949")
                            featureCollection = store.featureSource.features
                        } finally {
                            store.dispose()
                        }
                        if (featureCollection.isEmpty) {
                            println("${Config.local} 비어있는 파일: ${file.name}")
                            return@shpFile
                        }
                        pStmt.execute("TRUNCATE TABLE $tableCode")
                        pStmt.execute("SELECT SETVAL('public.${tableCode}_id_seq',1,false)")
                        val features = arrayOfNulls<SimpleFeature>(featureCollection.size())
                        featureCollection.toArray(features)
                        features.forEach feature@{ feature ->
                            val id = setupFtrIdn(feature!!)
                            val coordinate = setupCoordinate(feature)
                            val resultSet: ResultSet
                            try {
                                resultSet = mStmt.executeQuery("SELECT * FROM $tableCode WHERE ftr_idn=$id")
                            } catch (e: Exception) {
                                logger.error(e.message)
                                return@feature
                            }
                            resultSet.use { set ->
                                val metaData = set.metaData
                                val columnCount = metaData.columnCount

                                val columnNames = arrayOfNulls<String>(columnCount + 1)
                                columnNames[0] = "\"geom\""
                                for (i in 1..columnCount) {
                                    columnNames[i] = "\"${metaData.getColumnName(i).toLowerCase()}\""
                                }
                                val columnList = columnNames.joinToString(",").trim()

                                val columnValues = arrayOfNulls<String>(columnCount + 1)
                                while (resultSet.next()) {
                                    columnValues[0] = "st_geomfromtext('$coordinate', ${Config.origin})"
                                    for (j in 1..columnCount) {
                                        val field = ValueField(metaData.getColumnType(j), set.getString(j))
                                        columnValues[j] = field.value
                                    }
                                }
                                val valueList = columnValues.joinToString(",").trim()

                                val insertQuery = "INSERT INTO $tableCode ($columnList) VALUES ($valueList)"
                                if (file.name == "마을상수구역") insertQuery.replace("wsg_cde", "wsb_cde")
                                try {
                                    pStmt.execute(insertQuery)
                                } catch (e: PSQLException) {
                                    logger.error("($tableCode) ${e.message}")
                                    println("...${ANSI_RED}실패$ANSI_RESET")
                                } finally {
                                    postgres.commit()
                                }
                            }
                        }
                        println("...${ANSI_GREEN}완료$ANSI_RESET")
                    }
                    if (Config.isPostQuery) {
                        BufferedReader(FileReader("./postquery.txt")).use {
                            var line: String? = it.readLine()
                            while (line != null) {
                                try {
                                    line = it.readLine()?.trim()
                                    println(line)
                                    pStmt.execute(line)
                                } catch (e: PSQLException) {
                                    logger.error(e.message)
                                    println(line)
                                    return@open
                                } finally {
                                    postgres.commit()
                                }
                            }
                        }
                    }
                }
                println("${Config.local} 작업 종료: ${Date()}")
            }
        }
    }
}