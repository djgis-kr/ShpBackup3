package kr.djgis.shpbackup3

import java.io.File
import java.nio.charset.Charset
import kr.djgis.shpbackup3.network.PostgresConnectionPool
import kr.djgis.shpbackup3.property.Config
import kr.djgis.shpbackup3.property.Status
import kr.djgis.shpbackup3.property.at
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.data.simple.SimpleFeatureCollection
import org.opengis.feature.simple.SimpleFeature
import org.postgresql.util.PSQLException

class ExecuteShp(private val file: File) {

    private var errorCount = 0
    private val tableCode = file.nameWithoutExtension at tableList

    @Throws(Throwable::class)
    fun run() {
        val pConn = PostgresConnectionPool.getConnection()
        pConn.open(true) { postgres ->
            postgres.createStatement().use { pStmt ->
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
                val attributeCount = featureCollection.schema.attributeDescriptors.size
                val attributeDescriptors = featureCollection.schema.attributeDescriptors
                if (Status.tableCodeSet.add(tableCode)) {
                    pStmt.execute("TRUNCATE TABLE $tableCode")
                    pStmt.execute("SELECT SETVAL('public.${tableCode}_id_seq',1,false)")
                }
                val attributeNames = arrayOfNulls<String>(attributeCount)
                attributeNames[0] = "\"geom\""
                for (i in 1 until attributeCount) {
                    attributeNames[i] = "\"${attributeDescriptors[i].localName}\""
                }
                val columnList = attributeNames.joinToString(",").trim()
                features.forEach feature@{ feature ->
                    val columnValues = arrayOfNulls<String>(attributeCount)
                    val coordinate = setupCoordinate(feature!!)
                    columnValues[0] = "st_geomfromtext('$coordinate', ${Config.origin})"
                    for (j in 1 until attributeCount) {
                        val field = ValueField(null, feature.getAttribute(j).toString())
                        columnValues[j] = field.value
                    }
                    val valueList = columnValues.joinToString(",").trim()
                    val insertQuery = "INSERT INTO $tableCode ($columnList) VALUES ($valueList)"
                    try {
                        pStmt.execute(insertQuery)
                    } catch (e: PSQLException) {
                        errorCount += 1
                        logger.error("${file.nameWithoutExtension} $tableCode ${e.message}")
                    } finally {
                        postgres.commit()
                    }
                }
                when (errorCount) {
                    0 -> {
                        println("${Config.local} 정상 완료: ${file.nameWithoutExtension}")
                        logger.info("${file.nameWithoutExtension} $tableCode ${features.size - errorCount} rows")
                    }
                    in 1 until features.size -> {
                        println("${Config.local} 일부 에러: ${file.nameWithoutExtension}($errorCount)")
                        logger.info("${file.nameWithoutExtension} $tableCode ${features.size - errorCount} rows & $errorCount error(s).")
                    }
                    else -> {
                        postgres.rollback()
                        println("${Config.local} 전체 에러: ${file.nameWithoutExtension}($errorCount)...백업 취소 및 롤백 실행")
                    }
                }
            }
        }
    }
}
