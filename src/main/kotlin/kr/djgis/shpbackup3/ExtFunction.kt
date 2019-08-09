package kr.djgis.shpbackup3

import java.sql.Connection
import java.sql.Savepoint
import java.sql.Statement

@Throws(Throwable::class)
inline fun <R> Connection.open(rollback: Boolean = false, block: (Statement) -> R): R {
    try {
        var savepoint: Savepoint? = null
        if (rollback) {
            this.autoCommit = false
            savepoint = this.setSavepoint()
        }
        val stmt = this.createStatement()
        return try {
            block(stmt)
        } catch (e: Throwable) {
            this.rollback(savepoint)
            this.commit()
            throw e
        } finally {
            stmt.close()
        }
    } catch (e: Throwable) {
        throw e
    } finally {
        close()
    }
}