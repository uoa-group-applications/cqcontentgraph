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
     * Commit the currently active transaction
     *
     * @param conn is the connection to commit on
     */
    public void commit(Connection conn) throws SQLException {
       conn.commit();
    }

    /**
     * Rollback the transaction currently active on <code>conn</code>
     *
     * @param conn is the connection to rollback on
     */
    public boolean safeRollback(Connection conn) {
        try {
            conn.rollback();
        }
        catch (SQLException sqlEx) {
            LOG.error("Could not rollback transaction", sqlEx);
            return false;
        }
        return true;
    }

}
