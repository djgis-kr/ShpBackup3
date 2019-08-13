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
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileNotFoundException
import java.nio.charset.Charset
import java.sql.ResultSet
import java.util.*
import kotlin.collections.ArrayList

object Execute {

    // TODO: Apache CommonIO + 백업 주기 조절
    // TODO: 모든 작업 정상 종료 후 후실행.txt 명령 추가
    private var shpFiles: Array<File>? = null
    private var postgresQueries = ArrayList<String>()
    private lateinit var tableList: Properties
    val logger: Logger = LoggerFactory.getLogger(Execute.javaClass)

    fun run() = try {
        setupProperties()
        setupStatement()
    } catch (e: Exception) {
        logger.error(e.message)
        e.printStackTrace()
    }

    @Throws(Exception::class)
    private fun setupProperties() {
        tableList = initPropertyAt("./table.properties")
        shpFiles = File(Config.filePath).listFiles { file ->
            file.name.endsWith("shp") && file.length() > 100000L && (file.name at tableList != "")
        }
        shpFiles?.forEach {
            println(it.name)
        }
        if (shpFiles.isNullOrEmpty()) {
            throw FileNotFoundException(".shp File Not Found")
        }
    }

    @Throws(Throwable::class)
    private fun setupStatement() {
        val mConn = MysqlConnection.getConnection()
        val pConn = PostgresConnection.getConnection()
        mConn.createStatement().use { mStmt ->
            println("Mysql Connected")
            pConn.open(true) { postgres ->
                postgres.createStatement().use { pStmt ->
                    shpFiles?.forEach shpFile@{ file ->
                        println("처리시작: ${file.name}")
                        val tableCode = file.name at tableList
                        pStmt.execute("TRUNCATE TABLE $tableCode")
                        pStmt.execute("SELECT SETVAL('public.${tableCode}_id_seq',1,false)")
                        val store = ShapefileDataStore(file.toURI().toURL())
                        val featureCollection: SimpleFeatureCollection
                        try {
                            store.charset = Charset.forName("MS949")
                            featureCollection = store.featureSource.features
                        } finally {
                            store.dispose()
                        }
                        if (featureCollection.isEmpty) return@shpFile
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
//                                    logger.debug(insertQuery)
                                } finally {
                                    postgres.commit()
                                }
//                                postgresQueries.add(insertQuery)
                            }
                        }
//                        val length = postgresQueries.size
//                        for (i in 0 until length) try {
//                            pStmt.execute(postgresQueries[i])
//                        } catch (e: PSQLException) {
//                            logger.error("($tableCode) ${e.message}")
//                            logger.debug(postgresQueries[i])
//                            continue
//                        } finally {
//                            postgres.commit()
//                        }
                        println("처리함: ${file.name}")
                    }
                }
            }
        }
    }
}

private fun setupFeatures(file: File): Array<SimpleFeature>? {
    with(ShapefileDataStore(file.toURI().toURL())) {
        this.charset = Charset.forName("MS949")
        val featureCollection = this.featureSource.features
        this.dispose()
        when {
            featureCollection.isEmpty -> {
                return@setupFeatures null
            }
            else -> {
                val features = arrayOf<SimpleFeature>()
                featureCollection.toArray(arrayOf<SimpleFeature>())
                return@setupFeatures features
            }
        }
    }
}

private fun setupFtrIdn(feature: SimpleFeature): String {
    return when (feature.getProperty("관리번호")) {
        null -> feature.getProperty("FTR_IDN").value.toString()
        else -> feature.getProperty("관리번호").value.toString()
    }
}

private fun setupCoordinate(feature: SimpleFeature): String {
    return when ("MULTI" in "${feature.getAttribute(0)}") {
        false -> "MULTI${feature.getAttribute(0)}"
        true -> "${feature.getAttribute(0)}"
    }
}