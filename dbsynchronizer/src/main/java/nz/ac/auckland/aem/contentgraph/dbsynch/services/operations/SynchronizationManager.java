package nz.ac.auckland.aem.contentgraph.dbsynch.services.operations;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.SQLRunnable;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.Database;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper;

import javax.jcr.Node;
import java.sql.*;
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
    public boolean isBusy(Database db) throws SQLException {
        String state = getSynchState(db);
        return
            "reindexing".equals(state) ||
            "update".equals(state) ||
            "periodic_update".equals(state)
        ;
    }

    /**
     * @return true if the synchronisation is currently disabled.
     */
    public boolean isDisabled(Database db) throws SQLException {
        return "disabled".equals(getSynchState(db));
    }

    /**
     * Disable the synchronization
     *
     * @param db database connection
     */
    public void disable(Database db) throws SQLException {
        setSynchState(db, "disabled", "");
    }

    /**
     * Enable the synchronization
     *
     * @param db database connection
     */
    public void enable(Database db) throws SQLException {
        setSynchState(db, "operational", "");
    }

    /**
     * @return the current synchronization state stored in the database
     */
    protected String getSynchState(Database db) throws SQLException {
        return JDBCHelper.queryWithCallback(
                db.getConnection(),
                "SELECT state FROM SynchState ORDER BY id DESC LIMIT 1",
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
     * @param db
     * @param state
     * @param msg
     * @throws SQLException
     */
    protected void setSynchState(Database db, String state, String msg) throws SQLException {
        PreparedStatement pStmt =
                db.preparedStatement("INSERT INTO SynchState SET state = ?, msg = ?");

        pStmt.setString(1, state);
        pStmt.setString(2, msg);
        pStmt.executeUpdate();
    }

    public void startReindex(Database db) throws SQLException {
        this.setSynchState(db, "reindexing", "Complete re-index started");
    }

    /**
     * Indicate a periodic update is now taking place.
     */
    public void startPeriodicUpdate(Database db, Date from) throws SQLException {
        SimpleDateFormat sdFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        String fromTime = from == null ? "beginning of time" : sdFormat.format(from);
        this.setSynchState(db, "periodic_update", "Looking for changes since " + fromTime);
    }

    /**
     * Indicate that an update has started on node <code>node</code>
     */
    public void startUpdate(Database db, Node node) throws SQLException {
        this.setSynchState(db, "update", String.format("Updating `%s`", node));
    }

    /**
     * Indicate that an update has started on node <code>node</code>
     */
    public void startDelete(Database db, String nodePath) throws SQLException {
        this.setSynchState(db, "update", String.format("Deleting `%s`", nodePath));
    }

    /**
     * The process finished succesfully, flag is no longer set to busy.
     */
    public void finished(Database db) throws SQLException {
        this.setSynchState(db, "operational", "Operation completed successfully");
    }

    /**
     * This method is called when the reindex went wrong. But the
     * flag is no longer set to 'busy'. So another reindex can be fired.
     *
     * @param msg is the message to write to the database
     */
    public void finishedWithError(Database db, String msg) throws SQLException {
        this.setSynchState(db, "operational", "Error: " + msg);
    }
}
