package nz.ac.auckland.aem.contentgraph.dbsynch.periodic;

import nz.ac.auckland.aem.contentgraph.JcrChangeListener;
import nz.ac.auckland.aem.contentgraph.SynchronizationPaths;
import nz.ac.auckland.aem.contentgraph.dbsynch.DatabaseSynchronizer;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.SQLRunnable;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.ConnectionInfo;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.Database;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.visitors.DeleteSynchVisitor;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.visitors.PersistSynchVisitor;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.operations.SynchronizationManager;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.operations.TransactionManager;
import org.apache.felix.scr.annotations.*;
import org.apache.felix.scr.annotations.Properties;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.commons.scheduler.Scheduler;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;

import static nz.ac.auckland.aem.contentgraph.dbsynch.periodic.PathElement.PathOperation.Delete;
import static nz.ac.auckland.aem.contentgraph.dbsynch.periodic.PathElement.PathOperation.Update;

/**
 * @author Marnix Cook
 *
 * This class sets up the periodic database update
 */
@Service
@Component(
    immediate = true,
    metatype = true,
    name = "UoA Periodic Database Synchronizer"
)
@Properties({
    @Property(
        name = "nSeconds",
        label = "Every `n` seconds",
        description = "Attempt periodic update every `n` seconds (1 <= n <= 3600)",
        intValue = 5
    ),
    @Property(
        name = "trustQueue",
        label = "Trust the queue to be accurate?",
        description =
                "If enabled the changesets are only sourced from JCR change events, " +
                "otherwise intensive queries are executed to find the recent changes " +
                "(better use indexes!).",
        boolValue = true
    ),
    @Property(
        name = "enabled",
        label = "Enabled",
        description = "Service is enabled",
        boolValue = true
    )
})
public class PeriodicUpdateJobImpl implements PeriodicUpdateJob {

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(PeriodicUpdateJobImpl.class);

    /**
     * Minimum number of minutes
     */
    public static final int MIN_N_SECONDS = 1;

    /**
     * Maximum number of minutes
     */
    public static final int MAX_N_SECONDS = 60;

    /**
     * Default number of minutes
     */
    public static final int DEFAULT_N_SECONDS = 5;

    /**
     * Name of the job
     */
    public static final String JOB_NAME = "db_synch_periodic";

    /**
     * Is this service enabled?
     */
    private boolean enabled;

    /**
     * Number of minutes to wait between periodic schedulings
     */
    private Integer nSeconds = 5;

    /**
     * Do we trust the queue?
     */
    private Boolean trustQueue = true;

    @Reference
    private DatabaseSynchronizer dbSynch;

    /**
     * Synch workflow step
     */
    @Reference
    private JcrChangeListener synchPaths;

    @Reference
    private PathQueue pathQueue;

    /**
     * Scheduler
     */
    @Reference
    private Scheduler scheduler;

    @Reference
    private ResourceResolverFactory resolverFactory;
    private ResourceResolver resolver;
    private Session jcrSession;

    private SynchronizationManager sMgr = getSynchronizationManager();
    private TransactionManager txMgr = getTransactionManager();
    private PersistSynchVisitor updateVisitor = getUpdateVisitor();
    private DeleteSynchVisitor deleteVisitor = getDeleteSynchVisitor();



    /**
     * This method is called when the bundle is activated or when the bundle's configuration
     * has been modified. It will set this instance's data-members properly.
     *
     * @param context is the context to read the configuration from
     */
    @Activate @Modified
    public void configurationChanged(ComponentContext context) {
        this.nSeconds = getNormalizedSecondsConfiguration(context);
        this.enabled = (Boolean) context.getProperties().get("enabled");
        this.trustQueue = (Boolean) context.getProperties().get("trustQueue");

        startSession();

        try {
            removeExistingPeriodicJob();

            if (this.enabled) {
                this.scheduler.addPeriodicJob(JOB_NAME, this, null, this.nSeconds, false, true);
            }
            else {
                LOG.info("The periodic scheduler was disabled. Not going to be used.");
            }
        }
        catch (Exception ex) {
            LOG.error("Unfortunately the periodic job could not be scheduled");
        }
    }

    /**
     * Stop the session by logging out
     */
    @Deactivate
    public void stopSession() {
        if (this.jcrSession != null && this.jcrSession.isLive()) {
            this.jcrSession.logout();
        }
    }



    /**
     * Execute the periodic task that will determine the content that has
     * been written to the database since the last reindexing or periodic_update task.
     */
    @Override
    public void run() {
        if (this.dbSynch == null) {
            LOG.error("Database synchronizer reference not found.");
            return;
        }

        ConnectionInfo connInfo = this.dbSynch.getConnectionInfo();
        Connection dbConn = null;
        Database db = null;

        try {
            // connect
            dbConn = JDBCHelper.getDatabaseConnection(connInfo);
            if (dbConn == null) {
                LOG.info("No database connection, skipping.");
                return;
            }

            dbConn.setAutoCommit(false);
            db = new Database(dbConn, connInfo);

            if (sMgr.isBusy(db) || sMgr.isDisabled(db)) {
                LOG.info("Already busy or disabled, will skip this particular update");
                return;
            }

            // find last update
            Date lastUpdateAt = getLastUpdateDate(db.getConnection());

            if (lastUpdateAt == null) {
                LOG.warn("Not going to perform periodic update until a reindex was completed");
                return;
            }

            sMgr.startPeriodicUpdate(db, lastUpdateAt);

            // get all nodes that have changed since then
            Set<PathElement> queueElements = this.pathQueue.flushAndGet();
            doPathQueueUpdates(db, queueElements);

            if (!trustQueue) {
                NodeIterator nIterator = getNodesChangedSince(lastUpdateAt);
                synchronizeFromIterator(db, nIterator, queueElements);
            }

            // set state to 'finished'
            sMgr.finished(db);

            // commit transaction
            txMgr.commit(db.getConnection());
        }
        catch (Exception ex) {
            LOG.error("An SQL exception occurred", ex);
            if (db != null) {
                writeFinishedError(db, ex);
            }
            txMgr.safeRollback(dbConn);
        }
        finally {
            JDBCHelper.closeQuietly(dbConn);
        }
    }

    /**
     * Update elements that have been found by the JCR query.
     *
     * @param db
     * @param nIterator
     * @param queueElements
     * @throws Exception
     */
    protected void synchronizeFromIterator(Database db, NodeIterator nIterator, Set<PathElement> queueElements) throws Exception {
        // iterator not null? iterate.
        if (nIterator != null) {
            while (nIterator.hasNext()) {
                Node node = nIterator.nextNode();

                if (!shouldUpdate(node.getPath())) {
                    continue;
                }

                if (nodeInQueue(queueElements, node)) {
                    LOG.info("Was already synched from instant queue, not going to do it twice. Skipped.");
                    continue;
                }

                LOG.info("Periodic update for: " + node.getPath());
                this.updateVisitor.visit(db, node);
            }
        }
    }

    /**
     * @return if the node was in the queue was an update operation already
     */
    protected boolean nodeInQueue(Set<PathElement> queueElements, Node node) throws RepositoryException {
        return queueElements.contains(new PathElement(node.getPath(), Update));
    }

    /**
     * Execute the changes that were pushed into the path queue
     *
     * @param db the database connection to use
     * @param queueElements
     * @throws Exception when something has gone wrong
     */
    protected void doPathQueueUpdates(Database db, Set<PathElement> queueElements) throws Exception {
        for (PathElement pElement : queueElements) {

            // should even bother at all?
            if (!shouldUpdate(pElement.getPath())) {
                LOG.debug("`{}` not a tracked path, skipping.", pElement.getPath());
                continue;
            }

            // should be deleted?
            if (pElement.getOp() == Delete) {
                this.deleteVisitor.visit(db, pElement.getPath());
            }

            // should be updated?
            if (pElement.getOp() == Update) {
                Resource resource = this.resolver.getResource(pElement.getPath());
                if (resource == null) {
                    LOG.error("Could not update resource at `{}`, not found in repository", pElement.getPath());
                } else {
                    this.updateVisitor.visit(db, resource.adaptTo(Node.class));
                }
            }
        }
    }


    /**
     * Determine whether the path is updatable
     *
     * @param nodePath the node to check for validity
     * @return true if it's an updatable node
     * @throws RepositoryException
     */
    protected boolean shouldUpdate(String nodePath) throws RepositoryException {

        // if definitely an excluded path, skip it
        for (String excl : this.synchPaths.getExcludedPaths()) {
            if (nodePath.startsWith(excl)) {
                return false;
            }
        }

        // if an included path return true
        for (String incl : this.synchPaths.getIncludePaths()) {
            if (nodePath.startsWith(incl)) {
                return true;
            }
        }

        return false;

    }

    /**
     * Remove a job from the scheduler, if it doesn't exist, gracefully handle
     * exception that is thrown.
     */
    protected void removeExistingPeriodicJob() {
        try {
            this.scheduler.removeJob(JOB_NAME);
        }
        catch (NoSuchElementException nseEx) {
            LOG.info("Nothing was scheduled yet, all good, we'll just skip it.");
        }
    }

    /**
     * @return the setup number of minutes to wait for the next periodic job.
     */
    protected Integer getNormalizedSecondsConfiguration(ComponentContext context) {
        Integer cfgNMinutes = (Integer) context.getProperties().get("nSeconds");

        // not set? set default.
        if (cfgNMinutes == null) {
            cfgNMinutes = DEFAULT_N_SECONDS;
        }

        // make sure that is is within the predefined range
        cfgNMinutes =
            Math.max(
                    MIN_N_SECONDS,
                Math.min(
                        MAX_N_SECONDS,
                    cfgNMinutes
                )
            );

        return cfgNMinutes;
    }

    /**
     * Write an error to the synch status table
     */
    protected void writeFinishedError(Database db, Exception ex) {
        try {
            sMgr.finishedWithError(db, ex.getMessage());
        }
        catch (SQLException sqlEx2) {
            LOG.error("Could not log error result", sqlEx2);
        }
    }

    /**
     *
     * @param conn
     * @return
     * @throws SQLException
     */
    protected Date getLastUpdateDate(Connection conn) throws SQLException {
        return JDBCHelper.queryWithCallback(
                conn,
                "SELECT " +
                    "created_at " +
                    "FROM " +
                        "SynchState " +
                    "WHERE " +
                        "state = 'periodic_update' OR " +
                        "state = 'reindexing' " +
                    "ORDER BY created_at DESC " +
                    "LIMIT 1",

                Date.class,
                new SQLRunnable<Date>() {

                    @Override
                    public Date run(Statement stmt, ResultSet rSet) throws SQLException {
                        if (!rSet.next()) {
                            return null;
                        }
                        return rSet.getTimestamp(1);
                    }
                }
        );
    }

    /**
     * Retrieve all the nodes that have changed since <code>`since`</code>
     *
     * @param since the date to check for.
     * @return a node iterator
     */
    protected NodeIterator getNodesChangedSince(Date since) {
        String fmtSince = getJCRFormattedDate(since);

        try {
            QueryManager qMgr = jcrSession.getWorkspace().getQueryManager();
            Query query = qMgr.createQuery(
                String.format(
                    "SELECT p.* FROM [nt:base] AS p WHERE " +
                            "p.[jcr:created] >= CAST('%s' AS DATE) OR " +
                            "p.[jcr:lastModified] >= CAST('%s' AS DATE)",
                    fmtSince, fmtSince
                ),
                Query.JCR_SQL2
            );

            QueryResult qResult = query.execute();
            return qResult.getNodes();
        }
        catch (RepositoryException rEx) {
            LOG.error("A repository exception occurred", rEx);
        }

        return null;
    }

    /**
     * Get the JCR formatted date
     *
     * @param date is the date to convert to a JCR date
     * @return a JCR formatted date
     */
    protected String getJCRFormattedDate(Date date) {
        if (date == null) {
            date = new Date(0);
        }
        SimpleDateFormat sdFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.000'+12:00'");
        return sdFormat.format(date);
    }


    /**
     * @return the synchronization manager instance
     */
    protected SynchronizationManager getSynchronizationManager() {
        return new SynchronizationManager();
    }

    /**
     * @return the transaction manager instance
     */
    protected TransactionManager getTransactionManager() {
        return new TransactionManager();
    }

    /**
     * @return the synch visitor instance
     */
    protected PersistSynchVisitor getUpdateVisitor() {
        return new PersistSynchVisitor();
    }

    protected DeleteSynchVisitor getDeleteSynchVisitor() {
        return new DeleteSynchVisitor();
    }

    /**
     * @return a resource resolver instance, make sure to close it!
     */
    protected ResourceResolver getResourceResolver() {
        try {
            return this.resolverFactory.getAdministrativeResourceResolver(null);
        }
        catch (LoginException lEx) {
            LOG.error("Could not retrieve administration resource resolver", lEx);
            return null;
        }
    }

    /**
     * Start the JCR Session
     */
    protected void startSession() {

        // only start if not already started
        if (this.jcrSession != null) {
            return;
        }

        this.resolver = this.getResourceResolver();
        this.jcrSession = this.resolver.adaptTo(Session.class);
    }

}

