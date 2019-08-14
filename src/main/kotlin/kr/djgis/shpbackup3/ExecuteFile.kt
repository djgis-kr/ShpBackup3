package kr.djgis.shpbackup3

import kr.djgis.shpbackup3.network.MysqlConnection
import kr.djgis.shpbackup3.network.PostgresConnection
import kr.djgis.shpbackup3.property.Config
import kr.djgis.shpbackup3.property.at
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.data.simple.SimpleFeatureCollection
import org.opengis.feature.simple.SimpleFeature
import org.postgresql.util.PSQLException
import java.io.File
import java.nio.charset.Charset
import java.sql.ResultSet
import java.sql.SQLSyntaxErrorException
import java.util.concurrent.Callable

class ExecuteFile(private val file: File) : Callable<Nothing> {

    private val fileName = file.name.replace(".shp", "")
    private var errorCount = 0

    @Throws(Throwable::class)
    override fun call(): Nothing? {
        run()
        return null
    }

    @Throws(Throwable::class)
    private fun run() {
        val mConn = MysqlConnection().getConnection()
        val pConn = PostgresConnection().getConnection()
        mConn.createStatement().use { mStmt ->
            pConn.open(true) { postgres ->
                postgres.createStatement().use { pStmt ->
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
                        return
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
                        } catch (e: SQLSyntaxErrorException) {
                            errorCount += 1
                            logger.error("$fileName$ {e.message}")
                            logger.info("SELECT * FROM $tableCode WHERE ftr_idn=$id")
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
                                errorCount += 1
                                logger.error("($fileName, $tableCode) ${e.message}")
                            } finally {
                                postgres.commit()
                            }
                        }
                    }
                    when (errorCount) {
                        0 -> println("${Config.local} 백업 ${ANSI_GREEN}완료$ANSI_RESET: $fileName")
                        else -> println("${Config.local} 백업 ${ANSI_RED}에러: $fileName($errorCount)$ANSI_RESET")
                    }
                }
            }
        }
    }
}