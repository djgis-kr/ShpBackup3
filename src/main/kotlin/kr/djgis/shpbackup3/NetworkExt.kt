package kr.djgis.shpbackup3

import java.sql.Connection

@Throws(Throwable::class)
inline fun <R> Connection.open(rollback: Boolean = false, block: (Connection) -> R): R {
    try {
        if (rollback) {
            this.autoCommit = false
        }
        return block(this)
    } catch (e: Throwable) {
        throw e
    } finally {
        close()
    }
}