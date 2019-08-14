package kr.djgis.shpbackup3.network

import kr.djgis.shpbackup3.property.Config
import java.sql.Connection
import java.sql.DriverManager

class PostgresConnection {

    internal fun getConnection(
        dbCode: String = Config.dbCode,
        host: String = Config.pHost,
        port: String = Config.pPort,
        userName: String = Config.pUserName,
        password: String = Config.pKey
    ): Connection {
        Class.forName("org.postgresql.Driver")
        return DriverManager.getConnection("jdbc:postgresql://$host:$port/$dbCode", userName, password)
    }
}