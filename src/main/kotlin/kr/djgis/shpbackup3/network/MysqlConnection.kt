package kr.djgis.shpbackup3.network

import kr.djgis.shpbackup3.property.Config
import java.sql.Connection
import java.sql.DriverManager

object MysqlConnection {

    internal fun getConnection(
        dbCode: String = Config.dbCode,
        host: String = Config.mHost,
        port: String = Config.mPort,
        userName: String = Config.mUserName,
        password: String = Config.mKey
    ): Connection {
        println("jdbc:mysql://$host:$port/$dbCode?autoReconnect=true&user=$userName&password=$password&serverTimezone=UTC")
        Class.forName("com.mysql.cj.jdbc.Driver")
        return DriverManager.getConnection(
            "jdbc:mysql://$host:$port/$dbCode?autoReconnect=true&user=$userName&password=$password&serverTimezone=UTC"
        )
    }
}