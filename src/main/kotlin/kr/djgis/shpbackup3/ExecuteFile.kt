package kr.djgis.shpbackup3

import kr.djgis.shpbackup3.network.MysqlConnectionPool
import kr.djgis.shpbackup3.network.PostgresConnectionPool
import kr.djgis.shpbackup3.property.Config
import kr.djgis.shpbackup3.property.Status
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

    private var errorCount = 0

    @Throws(Throwable::class)
    override fun call(): Nothing? {
        val mConn = MysqlConnectionPool.getConnection()
        val pConn = PostgresConnectionPool.getConnection()
        mConn.createStatement().use { mStmt ->
            pConn.open(true) { postgres ->
                postgres.createStatement().use { pStmt ->
                    val tableCode = file.nameWithoutExtension at tableList
                    val featureCollection: SimpleFeatureCollection
                    val store = ShapefileDataStore(file.toURI().toURL())
                    try {
                        store.charset = Charset.forName("MS949")
                        featureCollection = store.featureSource.features
                    } finally {
                        store.featureSource?.unLockFeatures()
                        store.dispose()
                    }
                    val features = arrayOfNulls<SimpleFeature>(featureCollection.size())
                    featureCollection.toArray(features)
                    val metaData = mStmt.executeQuery("SELECT * FROM $tableCode LIMIT 0").metaData
                    val columnCount = metaData.columnCount
                    if (Status.tableCodeSet.add(tableCode)) {
                        pStmt.execute("TRUNCATE TABLE $tableCode")
                        pStmt.execute("SELECT SETVAL('public.${tableCode}_id_seq',1,false)")
                    }
                    features.forEach feature@{ feature ->
                        val ftrIdn = setupFtrIdn(feature!!)
                        val coordinate = setupCoordinate(feature)
                        val resultSet: ResultSet
                        try {
                            resultSet = mStmt.executeQuery("SELECT * FROM $tableCode WHERE ftr_idn=$ftrIdn")
                        } catch (e: SQLSyntaxErrorException) {
                            errorCount += 1
                            return@feature
                        }

                        val columnNames = arrayOfNulls<String>(columnCount + 1)
                        columnNames[0] = "\"geom\""
                        for (i in 1..columnCount) {
                            columnNames[i] = "\"${metaData.getColumnName(i).toLowerCase()}\""
                        }
                        val columnList = columnNames.joinToString(",").trim()

                        val columnValues = arrayOfNulls<String>(columnCount + 1)
                        columnValues[0] = "st_geomfromtext('$coordinate', ${Config.origin})"
                        while (resultSet.next()) for (j in 1..columnCount) {
                            val field = ValueField(metaData.getColumnType(j), resultSet.getString(j))
                            columnValues[j] = field.value
                        }
                        val valueList = columnValues.joinToString(",").trim()
                        resultSet.close()

                        val insertQuery = "INSERT INTO $tableCode ($columnList) VALUES ($valueList)"
                        if (file.nameWithoutExtension == "마을상수구역") insertQuery.replace("wsg_cde", "wsb_cde")
                        try {
                            pStmt.execute(insertQuery)
                        } catch (e: PSQLException) {
                            errorCount += 1
                            logger.error("(${file.nameWithoutExtension}, $tableCode($ftrIdn) ${e.message}")
                        } finally {
                            postgres.commit()
                        }
                    }
                    when (errorCount) {
                        0 -> println("${Config.local} 백업 완료: ${file.nameWithoutExtension}")
                        else -> println("${Config.local} 백업 에러: ${file.nameWithoutExtension}($errorCount)")
                    }
                }
            }
        }
        return null
    }
}