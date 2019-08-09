package kr.djgis.shpbackup3.network

import kr.djgis.shpbackup3.Config
import java.sql.Connection
import java.sql.DriverManager

object MysqlConn {

    internal fun getConnection(
        dbCode: String = Config.dbCode,
        host: String = Config.mHost,
        port: String = Config.mPort,
        userName: String = Config.mUserName,
        password: String = Config.mKey
    ): Connection {
        Class.forName("com.mysql.cj.jdbc.Driver")
        return DriverManager.getConnection(
            "jdbc:mysql://$host:$port/$dbCode?user=$userName&password=$password&serverTimezone=UTC&autoReconnect=true"
        )
    }
}