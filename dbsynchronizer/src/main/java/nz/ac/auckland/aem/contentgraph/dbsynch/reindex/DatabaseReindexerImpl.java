package nz.ac.auckland.aem.contentgraph.dbsynch.reindex;

import nz.ac.auckland.aem.contentgraph.JcrChangeListener;
import nz.ac.auckland.aem.contentgraph.SynchronizationPaths;
import nz.ac.auckland.aem.contentgraph.dbsynch.DatabaseSynchronizer;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dao.NodeDAO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dao.PropertyDAO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dto.PropertyDTO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.ConnectionInfo;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.Database;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.operations.PropertyConsumer;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

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

    private static final int N_CONSUMERS = 4;

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

    //
    //  Thread information
    //
    private List<PropertyConsumer> consumers;
    private BlockingQueue<List<PropertyDTO>> propertyQueue;


    /**
     * The runnable method
     */
    @Override
    public void run() {
        LOG.info("Starting to run the reindexer in the background.");

        ConnectionInfo connInfo = this.dbSynch.getConnectionInfo();
        Database database = null;

        NodeDAO.resetMapping();
        PerformanceReport.getInstance().resetMap();
        initializeConsumers(connInfo);

        try {
            database = new Database(connInfo);
            database.getConnection().setAutoCommit(false);

            long timestamp = System.currentTimeMillis();

            sMgr.startReindex(database);
            svMgr.reset();

            // remove all existing content.
            propertyDAO.truncate(database);
            nodeDAO.truncate(database);
            database.getConnection().commit();

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

            for (PropertyConsumer consumer : this.consumers) {
                consumer.commit();
            }

            // commit last bits
            database.getConnection().commit();
            database.getConnection().setAutoCommit(true);

            // set state to being 'finished'
            sMgr.finished(database);

            logPerformanceReport(timestamp, nNodes);
        }
        catch (Exception ex) {
            if (database != null) {
                txMgr.safeRollback(database.getConnection());
            }

            // write errors
            LOG.error("Something went wrong during the reindexing process. Finished with errors.", ex);
            writeErrorMessage(database, ex);
        }
        finally {
            if (database != null) {
                JDBCHelper.closeQuietly(database.getConnection());
            }
        }

    }

    /**
     * Log the performance stats that have been gathered during the running
     * of the reindexer.
     *
     * @param timestamp is the starting timestamp
     * @param nNodes is the number of nodes
     */
    protected void logPerformanceReport(long timestamp, int nNodes) {
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
                String.format("%-40s: %.2f", spent.getKey(), spent.getValue() * 0.001)
            );
        }
    }

    /**
     * Initialize the consumers
     *
     * @param connInfo
     */
    protected void initializeConsumers(ConnectionInfo connInfo) {
        if (consumers == null) {
            consumers = createConsumers(connInfo, N_CONSUMERS);
        }
    }

    /**
     * Write an error message to the synchstate table.
     */
    protected void writeErrorMessage(Database db, Exception ex) {
        if (db != null) {
            try {
                sMgr.finishedWithError(db, ex.getMessage());
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


    /**
     * Create a list of threads (that have been started) with a new database connection.
     *
     * @param connInfo
     * @param nConsumers
     * @return
     */
    protected List<PropertyConsumer> createConsumers(ConnectionInfo connInfo, int nConsumers) {
        List<PropertyConsumer> consumers = new ArrayList<PropertyConsumer>();

        for (int idx = 0; idx < nConsumers; ++idx) {
            try {
                Connection dbConn = JDBCHelper.getDatabaseConnection(connInfo);
                dbConn.setAutoCommit(false);
                Database database = new Database(dbConn, connInfo);

                PropertyConsumer propConsumer =
                        new PropertyConsumer(
                                database,
                                getPropertyQueue()
                        );

                Thread consumerThread = new Thread(propConsumer);
                consumerThread.setName("Consumer #" + (idx + 1));
                consumerThread.start();

                consumers.add(propConsumer);
            }
            catch (SQLException sqlEx) {
                LOG.error("Cannot create consumer thread", sqlEx);
            }
        }

        return consumers;
    }


    // ------------------------------------------------------------------------
    //    Class seam definition
    // ------------------------------------------------------------------------

    protected BlockingQueue<List<PropertyDTO>> getPropertyQueue() {
        if (propertyQueue == null) {
            propertyQueue = new LinkedBlockingQueue<List<PropertyDTO>>();
        }
        return propertyQueue;
    }

    protected SynchVisitor<Node> getSynchVisitorInstance() {
        return new ReindexPersistSynchVisitor(getPropertyQueue());
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
