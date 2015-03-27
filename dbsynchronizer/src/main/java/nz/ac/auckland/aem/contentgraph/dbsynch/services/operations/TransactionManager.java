package nz.ac.auckland.aem.contentgraph.dbsynch.services.operations;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Marnix Cook
 *
 * Contains transaction operations
 */
public class TransactionManager {

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(TransactionManager.class);

    /**
     * Start a transaction on <code>conn</code>
     *
     * @param conn is the connection to start a transacton on
     */
    public void start(Connection conn) throws SQLException {
        JDBCHelper.query(conn, "START TRANSACTION");
    }

    /**
     * Commit the currently active transaction
     *
     * @param conn is the connection to commit on
     */
    public void commit(Connection conn) throws SQLException {
        JDBCHelper.query(conn, "COMMIT");
    }

    /**
     * Rollback the transaction currently active on <code>conn</code>
     *
     * @param conn is the connection to rollback on
     */
    public boolean rollback(Connection conn) {
        try {
            JDBCHelper.query(conn, "ROLLBACK");
        }
        catch (SQLException sqlEx) {
            LOG.error("Could not rollback transaction", sqlEx);
            return false;
        }
        return true;
    }

}
