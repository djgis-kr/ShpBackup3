package kr.djgis.shpbackup3

import java.sql.Connection
import java.sql.Types
import java.text.ParseException
import java.text.SimpleDateFormat

@Throws(Throwable::class)
inline fun <R> Connection.open(rollback: Boolean = false, block: (Connection) -> R): R {
    try {
        if (rollback) {
            this.autoCommit = false
        }
        return block(this)
    } catch (e: Throwable) {
        e.printStackTrace()
        throw e
    } finally {
        close()
    }
}

infix fun String.add(value: String): String {
    return this + value
}

class ValueField(private val columnType: Int, private val columnValue: String?) {

    private val dateFormat = SimpleDateFormat("yyyy-MM-dd")

    /**
     * @return columnType 을 받아 SQL query syntax 에 맞게 가공
     */
    val value: String
        @Throws(ParseException::class)
        get() = when {
            columnValue != null -> {
                val dataFormat: String
                var dataValue: String = columnValue
                when (columnType) {
                    Types.TIMESTAMP, Types.DATE -> {
                        dataFormat = "'%s'"
                        dataValue = dateFormat.format(dateFormat.parse(columnValue))
                    }
                    Types.INTEGER, Types.DECIMAL, Types.DOUBLE -> {
                        dataFormat = "%s"
                    }
                    Types.CHAR, Types.VARCHAR, Types.TIME -> {
                        dataFormat = "'%s'"
                    }
                    else -> dataFormat = "'%s'"
                }
                String.format(dataFormat, dataValue)
            }
            else -> {
                "null"
            }
        }
}