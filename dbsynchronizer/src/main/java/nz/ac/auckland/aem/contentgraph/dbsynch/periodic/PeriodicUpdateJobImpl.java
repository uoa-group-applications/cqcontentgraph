package nz.ac.auckland.aem.contentgraph.dbsynch.periodic;

import nz.ac.auckland.aem.contentgraph.dbsynch.DatabaseSynchronizer;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.SQLRunnable;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.ConnectionInfo;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.Database;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.visitors.PersistSynchVisitor;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.visitors.SynchVisitor;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.operations.SynchronizationManager;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.operations.TransactionManager;
import nz.ac.auckland.aem.contentgraph.workflow.SynchWorkflowStep;
import org.apache.felix.scr.annotations.*;
import org.apache.felix.scr.annotations.Properties;
import org.apache.sling.api.resource.LoginException;
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
        name = "nMinutes",
        label = "Every `n` minutes",
        description = "Attempt periodic update every `n` minutes (1 <= n <= 60)",
        intValue = 5
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
    public static final int MIN_N_MINUTES = 1;

    /**
     * Maximum number of minutes
     */
    public static final int MAX_N_MINUTES = 60;

    /**
     * Default number of minutes
     */
    public static final int DEFAULT_N_MINUTES = 5;

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
    private Integer nMinutes = 5;


    @Reference
    private DatabaseSynchronizer dbSynch;

    /**
     * Synch workflow step
     */
    @Reference
    private SynchWorkflowStep synchWorkflowStep;

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
    private SynchVisitor<Node> synchVisitor = getSynchVisitor();



    /**
     * This method is called when the bundle is activated or when the bundle's configuration
     * has been modified. It will set this instance's data-members properly.
     *
     * @param context is the context to read the configuration from
     */
    @Activate @Modified
    public void configurationChanged(ComponentContext context) {
        this.nMinutes = getNormalizedMinutesConfiguration(context);
        this.enabled = (Boolean) context.getProperties().get("enabled");

        startSession();

        try {
            removeExistingPeriodicJob();

            if (this.enabled) {
                this.scheduler.addPeriodicJob(JOB_NAME, this, null, this.nMinutes/* * 60*/, false, true);
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
        ConnectionInfo connInfo = this.dbSynch.getConnectionInfo();
        Connection dbConn = null;

        try {
            // connect
            dbConn = JDBCHelper.getDatabaseConnection(connInfo);
            dbConn.setAutoCommit(false);

            if (sMgr.isBusy(dbConn) || sMgr.isDisabled(dbConn)) {
                LOG.info("Already busy or disabled, will skip this particular update");
                return;
            }

            Database db = new Database(dbConn);

            // find last update
            Date lastUpdateAt = getLastUpdateDate(dbConn);

            if (lastUpdateAt == null) {
                LOG.warn("Not going to perform periodic update until a reindex was completed");
                return;
            }

            sMgr.startPeriodicUpdate(dbConn, lastUpdateAt);

            // get all nodes that have changed since then
            NodeIterator nIterator = getNodesChangedSince(lastUpdateAt);

            // iterator not null? iterate.
            if (nIterator != null) {
                while (nIterator.hasNext()) {
                    Node node = nIterator.nextNode();

                    if (!shouldUpdate(node)) {
                        continue;
                    }
                    LOG.info("Periodic update for: " + node.getPath());

                    this.synchVisitor.visit(db, node);
                }
            }

            // set state to 'finished'
            sMgr.finished(dbConn);

            // commit transaction
            txMgr.commit(dbConn);
        }
        catch (Exception ex) {
            LOG.error("An SQL exception occurred", ex);
            if (dbConn != null) {
                writeFinishedError(dbConn, ex);
            }
            txMgr.safeRollback(dbConn);
        }
        finally {
            JDBCHelper.closeQuietly(dbConn);
        }
    }


    /**
     * Determine whether the path is updatable
     *
     * @param node the node to check for validity
     * @return true if it's an updatable node
     * @throws RepositoryException
     */
    protected boolean shouldUpdate(Node node) throws RepositoryException {
        String nodePath = node.getPath();

        // if definitely an excluded path, skip it
        for (String excl : this.synchWorkflowStep.getExcludedPaths()) {
            if (nodePath.startsWith(excl)) {
                return false;
            }
        }

        // if an included path return true
        for (String incl : this.synchWorkflowStep.getIncludePaths()) {
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
    protected Integer getNormalizedMinutesConfiguration(ComponentContext context) {
        Integer cfgNMinutes = (Integer) context.getProperties().get("nMinutes");

        // not set? set default.
        if (cfgNMinutes == null) {
            cfgNMinutes = DEFAULT_N_MINUTES;
        }

        // make sure that is is within the predefined range
        cfgNMinutes =
            Math.max(
                MIN_N_MINUTES,
                Math.min(
                    MAX_N_MINUTES,
                    cfgNMinutes
                )
            );

        return cfgNMinutes;
    }

    /**
     * Write an error to the synch status table
     */
    protected void writeFinishedError(Connection dbConn, Exception ex) {
        try {
            sMgr.finishedWithError(dbConn, ex.getMessage());
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
    protected SynchVisitor<Node> getSynchVisitor() {
        return new PersistSynchVisitor();
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

