package kr.djgis.shpbackup3

object Config {

    lateinit var local: String
    lateinit var dbCode: String
    lateinit var filePath: String
    lateinit var mHost: String
    lateinit var mPort: String
    lateinit var mUserName: String
    lateinit var mKey: String
    lateinit var pHost: String
    lateinit var pPort: String
    lateinit var pUserName: String
    lateinit var pKey: String
    lateinit var origin: String

    private val property = Preferences.get()

    init {
        this.apply {
            local = property of "local"
            dbCode = property of "dbCode"
            filePath = property of "shp_path"
            mHost = property of "mysql_host"
            mPort = property of "mysql_port"
            mUserName = property of "mysql_username"
            mKey = property of "mysql_key"
            pHost = property of "postgres_host"
            pPort = property of "postgres_port"
            pUserName = property of "postgres_username"
            pKey = property of "postgres_key"
            origin = property of "geo_origin"
        }
    }
}