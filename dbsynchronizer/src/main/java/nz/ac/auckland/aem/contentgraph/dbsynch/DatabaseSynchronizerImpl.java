package nz.ac.auckland.aem.contentgraph.dbsynch;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.ConnectionInfo;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.Database;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.visitors.DeleteSynchVisitor;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.visitors.PersistSynchVisitor;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.visitors.SynchVisitor;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.operations.SynchronizationManager;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.operations.TransactionManager;
import org.apache.felix.scr.annotations.*;
import org.apache.sling.api.resource.Resource;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import java.sql.*;

import static nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper.*;
import static nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper.closeQuietly;

/**
 * @author Marnix Cook
 *
 * This class is an implementation of the synchronizer interface and is invoked
 * whenever a page is published to the publication servers. When the page is published
 * certain actions can be taken. In this case, we'll be updating a database so
 * we can do cool queries on the new content.
 */
@Service
@Component(
    immediate = true,
    metatype = true,
    name = "UoA Database Content Synchronizer"
)
@Properties({
    @Property(
        name = DatabaseSynchronizer.BUNDLE_PARAM_ENABLED,
        label = "Enabled",
        description = "Ticked if the database synchronizer is enabled",
        boolValue = true
    ),
    @Property(
        name = DatabaseSynchronizer.BUNDLE_PARAM_JDBC,
        label = "JDBC Connection string",
        description = "Contains the JDBC connection string"
    ),
    @Property(
        name = DatabaseSynchronizer.BUNDLE_PARAM_USER,
        label = "Username",
        description = "Database username"
    ),
    @Property(
        name = DatabaseSynchronizer.BUNDLE_PARAM_PASSWORD,
        label = "Password",
        description = "Database password"
    )
})
public class DatabaseSynchronizerImpl implements DatabaseSynchronizer {

    // -----------------------------------------------------------------
    //      Bundle parameter constants
    // -----------------------------------------------------------------

    private TransactionManager txMgr = getTxMgrInstance();
    private SynchronizationManager sMgr = new SynchronizationManager();

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseSynchronizerImpl.class);

    /**
     * Enabled
     */
    private boolean enabled;

    /**
     * Connection information
     */
    private ConnectionInfo connInfo;

    /**
     * Persist visitor
     */
    private SynchVisitor<Node> persistVisitor = getPersistSynchVisitor();

    /**
     * Delete visitor
     */
    private SynchVisitor<String> deleteVisitor = getDeleteSynchVisitor();



    /**
     * Called when the configuration changed
     *
     * @param context
     */
    @Activate @Modified
    public void configChanged(ComponentContext context) {
        this.enabled = (Boolean) context.getProperties().get(BUNDLE_PARAM_ENABLED);
        this.connInfo = getConnectionInformation(context);

        if (!loadJdbcDriver() || this.connInfo.isIncomplete()) {
            this.enabled = false;
        }
    }


    /**
     * @return a connection information instance that
     */
    protected ConnectionInfo getConnectionInformation(ComponentContext context) {
        return new ConnectionInfo(
            (String) context.getProperties().get(BUNDLE_PARAM_JDBC),
            (String) context.getProperties().get(BUNDLE_PARAM_USER),
            (String) context.getProperties().get(BUNDLE_PARAM_PASSWORD)
        );
    }


    /**
     * Called when a page is created or updated
     *
     * @param resource the resource that has been changed
     */
    @Override
    public void synch(Resource resource) {
        if (!this.enabled) {
            LOG.info("Database synchronization not enabled, stopping synch");
            return;
        }

        LOG.info("Synching the database for resource: " + resource.getPath());

        Node jcrNode = resource.adaptTo(Node.class);

        Connection dbConn = null;

        try {
            dbConn = getDatabaseConnection(this.connInfo);
            Database database = new Database(dbConn);

            // can we write right now? if not, end.
            if (sMgr.isBusy(dbConn) || sMgr.isDisabled(dbConn)) {
                LOG.error("Cannot update, synchronizer is currently busy or disabled");
                return;
            }

            // set state to update.
            sMgr.startUpdate(dbConn, jcrNode);
            this.persistVisitor.visit(database, jcrNode);
            sMgr.finished(dbConn);
        }
        catch (Exception ex) {
            rollback(dbConn);
            writeFinishedWithError(dbConn, ex.getMessage());
        }
        finally {
            closeQuietly(dbConn);
        }

    }


    /**
     * Called when a page or asset has been deleted.
     *
     * @param path the path that is no more
     */
    @Override
    public void delete(String path) {
        if (!this.enabled) {
            LOG.info("Database synchronization not enabled, stopping synch");
            return;
        }

        Connection dbConn = null;

        try {
            dbConn = getDatabaseConnection(this.connInfo);
            Database database = new Database(dbConn);

            // can we write right now? if not, end.
            if (sMgr.isBusy(dbConn) || sMgr.isDisabled(dbConn)) {
                LOG.error("Cannot update, synchronizer is currently busy or disabled");
                return;
            }

            // set state to update.
            sMgr.startDelete(dbConn, path);
            this.deleteVisitor.visit(database, path);
            sMgr.finished(dbConn);
        }
        catch (Exception ex) {
            LOG.error("An error occurred", ex);
            rollback(dbConn);
        }
        finally {
            closeQuietly(dbConn);
        }
    }

    /**
     * Rollback the connection
     *
     * @param conn is the connection instance
     */
    protected void rollback(Connection conn) {
        if (conn != null) {
            if (!txMgr.safeRollback(conn)) {
                LOG.error("Rollback failed!");
            }
        } else {
            LOG.info("Cannot rollback, connection was already closed");
        }
    }

    /**
     * Write the error to the database
     */
    protected void writeFinishedWithError(Connection conn, String error) {
        try {
            sMgr.finishedWithError(conn, error);
        }
        catch (SQLException sqlEx) {
            LOG.error("Could not write finish status to database, queue will be broken.", sqlEx);
        }
    }



    public ConnectionInfo getConnectionInfo() {
        return this.connInfo;
    }

    // ----------------------------------------------------------------------------------------
    //      Getters for instances that might define the seam of this class
    // ----------------------------------------------------------------------------------------

    protected TransactionManager getTxMgrInstance() {
        return new TransactionManager();
    }

    protected SynchVisitor getPersistSynchVisitor() {
        return new PersistSynchVisitor();
    }

    protected DeleteSynchVisitor getDeleteSynchVisitor() {
        return new DeleteSynchVisitor();
    }
}
