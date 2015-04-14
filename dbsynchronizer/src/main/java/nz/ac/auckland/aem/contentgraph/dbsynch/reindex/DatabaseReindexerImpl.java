package nz.ac.auckland.aem.contentgraph.dbsynch.reindex;

import nz.ac.auckland.aem.contentgraph.JcrChangeListener;
import nz.ac.auckland.aem.contentgraph.SynchronizationPaths;
import nz.ac.auckland.aem.contentgraph.dbsynch.DatabaseSynchronizer;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dao.NodeDAO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dao.PropertyDAO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.ConnectionInfo;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.Database;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.visitors.PersistSynchVisitor;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.visitors.ReindexPersistSynchVisitor;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.visitors.SynchVisitor;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.operations.SynchVisitorManager;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.operations.SynchronizationManager;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.operations.TransactionManager;
import nz.ac.auckland.aem.contentgraph.utils.PerformanceReport;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author Marnix Cook
 *
 * Implementation of database reindex interface. Is able to recursively visit
 * a select number of nodes.
 */
@Service
@Component(immediate = true)
public class DatabaseReindexerImpl implements DatabaseReindexer {

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseReindexerImpl.class);

    @Reference
    private JcrChangeListener synchPaths;

    /**
     * Necessary to get the database connection information
     */
    @Reference
    private DatabaseSynchronizer dbSynch;

    /**
     * Resource resolver factory.
     */
    @Reference
    private ResourceResolverFactory rrFactory;

    /**
     * Resource resolver
     */
    private ResourceResolver resourceResolver;

    private TransactionManager txMgr = getTransactionManager();
    private SynchronizationManager sMgr = getSynchronizationManagerInstance();
    private SynchVisitorManager svMgr = getSynchVisitorManager();

    private SynchVisitor sVisitor = getSynchVisitorInstance();
    private PropertyDAO propertyDAO = getPropertyDAO();
    private NodeDAO nodeDAO = getNodeDAO();


    /**
     * The runnable method
     */
    @Override
    public void run() {
        LOG.info("Starting to run the reindexer in the background.");

        ConnectionInfo connInfo = this.dbSynch.getConnectionInfo();
        Connection dbConn = null;

        PerformanceReport.getInstance().resetMap();

        try {
            dbConn = JDBCHelper.getDatabaseConnection(connInfo);
            dbConn.setAutoCommit(false);

            Database database = new Database(dbConn);

            long timestamp = System.currentTimeMillis();

            sMgr.startReindex(dbConn);
            svMgr.reset();

            // remove all existing content.
            propertyDAO.truncate(database);
            nodeDAO.truncate(database);

            // commit truncation of information
            txMgr.commit(dbConn);

            int nNodes = 0;

            // iterate over all base paths
            for (String includePath : this.synchPaths.getIncludePaths()) {
                Resource inclResource = this.getResourceResolver().getResource(includePath);
                if (inclResource == null) {
                    LOG.error("Could not find `{}`, skipping", includePath);
                    continue;
                }

                // node
                Node inclNode = inclResource.adaptTo(Node.class);

                // recursion
                nNodes += svMgr.recursiveVisit(database, inclNode, this.synchPaths.getExcludedPaths(), this.sVisitor);
            }

            // commit last properties
            new PropertyDAO().executeBatch(database);

            // commit last bits
            txMgr.commit(dbConn);

            dbConn.setAutoCommit(true);

            // set state to being 'finished'
            sMgr.finished(dbConn);

            long doneStamp = System.currentTimeMillis();

            LOG.info(
                String.format(
                    "Successfully finished the re-indexing process, took %.2f seconds for %d nodes.",
                    ((doneStamp - timestamp) * 0.001),
                    nNodes
                )
            );

            // output all
            for (Map.Entry<String, Long> spent : PerformanceReport.getInstance().getMap().entrySet()) {
                LOG.info(
                        String.format("%40s: %.2f", spent.getKey(), spent.getValue() * 0.001)
                );
            }
        }
        catch (Exception ex) {
            txMgr.safeRollback(dbConn);

            // write errors
            LOG.error("Something went wrong during the reindexing process. Finished with errors.", ex);
            writeErrorMessage(dbConn, ex);
        }
        finally {
            JDBCHelper.closeQuietly(dbConn);
        }

    }

    /**
     * Write an error message to the synchstate table.
     */
    protected void writeErrorMessage(Connection dbConn, Exception ex) {
        if (dbConn != null) {
            try {
                sMgr.finishedWithError(dbConn, ex.getMessage());
            }
            catch (SQLException sqlEx) {
                LOG.error("Cannot reset the state, queue will probably malfunction from now .. !", sqlEx);
            }
        }
    }

    /**
     * @return the synchronization manager instance
     */
    protected SynchronizationManager getSynchronizationManagerInstance() {
        return new SynchronizationManager();
    }

    /**
     * @return the resource resolver instance or null when not found.
     */
    protected ResourceResolver getResourceResolver() {
        try {
            if (this.resourceResolver == null) {
                this.resourceResolver = this.rrFactory.getAdministrativeResourceResolver(null);
            }
            return this.resourceResolver;
        }
        catch (LoginException lEx) {
            LOG.error("Cannot login to retrieve resource resolver", lEx);
        }

        return null;
    }

    // ------------------------------------------------------------------------
    //    Class seam definition
    // ------------------------------------------------------------------------

    protected SynchVisitor<Node> getSynchVisitorInstance() {
        return new ReindexPersistSynchVisitor();
    }

    protected NodeDAO getNodeDAO() {
        return new NodeDAO();
    }

    protected TransactionManager getTransactionManager() {
        return new TransactionManager();
    }

    protected PropertyDAO getPropertyDAO() {
        return new PropertyDAO();
    }

    protected SynchVisitorManager getSynchVisitorManager() {
        return new SynchVisitorManager();
    }

}
