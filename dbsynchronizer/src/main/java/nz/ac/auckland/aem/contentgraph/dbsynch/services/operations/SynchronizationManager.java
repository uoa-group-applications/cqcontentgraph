package nz.ac.auckland.aem.contentgraph.dbsynch.services.operations;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.SQLRunnable;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper;

import javax.jcr.Node;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import static nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper.escape;

/**
 * @author Marnix Cook
 *
 * This manager is able to look into the database and determine whether
 * a reindexing process is currently underway.
 */
public class SynchronizationManager {

    /**
     * @return true if synchronization is currently underway
     */
    public boolean isBusy(Connection conn) throws SQLException {
        String state = getSynchState(conn);
        return
            "reindexing".equals(state) ||
            "update".equals(state) ||
            "periodic_update".equals(state)
        ;
    }

    /**
     * @return true if the synchronisation is currently disabled.
     */
    public boolean isDisabled(Connection conn) throws SQLException {
        return "disabled".equals(getSynchState(conn));
    }

    /**
     * Disable the synchronization
     *
     * @param conn database connection
     */
    public void disable(Connection conn) throws SQLException {
        setSynchState(conn, "disabled", "");
    }

    /**
     * Enable the synchronization
     *
     * @param conn database connection
     */
    public void enable(Connection conn) throws SQLException {
        setSynchState(conn, "operational", "");
    }

    /**
     * @return the current synchronization state stored in the database
     */
    protected String getSynchState(Connection conn) throws SQLException {
        return JDBCHelper.queryWithCallback(
                conn,
                "SELECT state FROM SynchState ORDER BY created_at DESC LIMIT 1",
                String.class,
                new SQLRunnable<String>() {

                    @Override
                    public String run(Statement stmt, ResultSet rSet) throws SQLException {
                        if (rSet.next()) {
                            return rSet.getString(1);
                        }
                        return null;
                    }
                }
        );
    }

    /**
     * Set the synchronisation state to a specific state, optional message can be inserted as well
     *
     * @param conn
     * @param state
     * @param msg
     * @throws SQLException
     */
    protected void setSynchState(Connection conn, String state, String msg) throws SQLException {
        JDBCHelper.updateWithCallback(
            conn,
            String.format(
                "INSERT INTO SynchState SET state = '%s', msg = '%s'",
                escape(state), escape(msg)
            ),
            Void.class,
            null
        );
    }

    public void startReindex(Connection conn) throws SQLException {
        this.setSynchState(conn, "reindexing", "Complete re-index started");
    }

    /**
     * Indicate a periodic update is now taking place.
     */
    public void startPeriodicUpdate(Connection conn, Date from) throws SQLException {
        SimpleDateFormat sdFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        String fromTime = from == null ? "beginning of time" : sdFormat.format(from);
        this.setSynchState(conn, "periodic_update", "Looking for changes since " + fromTime);
    }

    /**
     * Indicate that an update has started on node <code>node</code>
     */
    public void startUpdate(Connection conn, Node node) throws SQLException {
        this.setSynchState(conn, "update", String.format("Updating `%s`", node));
    }

    /**
     * Indicate that an update has started on node <code>node</code>
     */
    public void startDelete(Connection conn, String nodePath) throws SQLException {
        this.setSynchState(conn, "update", String.format("Deleting `%s`", nodePath));
    }

    /**
     * The process finished succesfully, flag is no longer set to busy.
     */
    public void finished(Connection conn) throws SQLException {
        this.setSynchState(conn, "operational", "Operation completed successfully");
    }

    /**
     * This method is called when the reindex went wrong. But the
     * flag is no longer set to 'busy'. So another reindex can be fired.
     *
     * @param msg is the message to write to the database
     */
    public void finishedWithError(Connection conn, String msg) throws SQLException {
        this.setSynchState(conn, "operational", "Error: " + msg);
    }
}
