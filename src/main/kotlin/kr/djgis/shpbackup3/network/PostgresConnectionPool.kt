package kr.djgis.shpbackup3.network

import kr.djgis.shpbackup3.property.Config
import org.apache.commons.dbcp2.BasicDataSource
import java.sql.Connection
import java.sql.SQLException

object PostgresConnectionPool {

    private val source = BasicDataSource()

    init {
        source.apply {
            driverClassName = "org.postgresql.Driver"
            url = "jdbc:postgresql://${Config.pHost}:${Config.pPort}/${Config.dbCode}"
            username = Config.pUserName
            password = Config.pKey
            maxIdle = Config.maxIdle
            maxTotal = Config.maxTotal
            minEvictableIdleTimeMillis = -1
        }
    }

    @Throws(SQLException::class)
    fun getConnection(): Connection {
        return source.connection
    }
}