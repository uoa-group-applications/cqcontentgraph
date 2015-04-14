package nz.ac.auckland.aem.contentgraph.dbsynch;

import nz.ac.auckland.aem.contentgraph.dbsynch.reindex.DatabaseReindexer;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.ConnectionInfo;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.Database;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.operations.SynchronizationManager;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.sling.SlingServlet;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.servlets.SlingSafeMethodsServlet;
import org.apache.sling.commons.scheduler.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Marnix Cook
 *
 * This servlet will trigger the reindexing of the entire database. It will
 * return a '423 Locked' when the reindexing is already going on. Otherwise
 * a '200 OK' is returned.
 *
 * More information about http response codes:
 * http://en.wikipedia.org/wiki/List_of_HTTP_status_codes
 */
@SlingServlet(paths = "/bin/contentgraph/reindex.do", methods = "GET")
public class ReindexTriggerServlet extends SlingSafeMethodsServlet {

    public static final int STATUS_LOCKED = 423;

    /**
     * Hook up Scheduler instance.
     */
    @Reference
    private Scheduler scheduler;

    @Reference
    private DatabaseReindexer dbReindexer;

    @Reference
    private DatabaseSynchronizer dbSynch;

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(ReindexTriggerServlet.class);

    /**
     * Status manager
     */
    private SynchronizationManager synchronizationManager = getSynchronizationManager();

    /**
     * Implementation of the GET request.
     *
     * @param request
     * @param response
     * @throws ServletException
     * @throws IOException
     */
    @Override
    protected void doGet(SlingHttpServletRequest request, SlingHttpServletResponse response) throws ServletException, IOException {

        Database db = null;
        ConnectionInfo connInfo = this.dbSynch.getConnectionInfo();

        try {
            db = new Database(connInfo);

            if (synchronizationManager.isDisabled(db) || synchronizationManager.isBusy(db)) {
                response.setStatus(STATUS_LOCKED);
                return;
            }

            // OK, start reindexing job.
            if (this.scheduleReindex()) {
                response.setStatus(HttpServletResponse.SC_OK);
            } else {
                response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            }
        }
        catch (SQLException sqlEx) {
            LOG.error("Got SQL exception", sqlEx);
        }
        finally {
            if (db != null) {
                JDBCHelper.closeQuietly(db.getConnection());
            }
        }
    }

    /**
     * @return true if the job was scheduled properly.
     */
    protected boolean scheduleReindex() {
        try {
            scheduler.fireJob(this.dbReindexer, null);
            return true;
        }
        catch (Exception ex) {
            LOG.error("Cannot schedule the fire job, because:", ex);
            return false;
        }
    }

    /**
     * @return the status manager instance (part of class seam)
     */
    protected SynchronizationManager getSynchronizationManager() {
        return new SynchronizationManager();
    }
}
