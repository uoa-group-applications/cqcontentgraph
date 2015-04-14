package nz.ac.auckland.aem.contentgraph.dbsynch;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.ConnectionInfo;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.Database;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.operations.SynchronizationManager;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.sling.SlingServlet;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.servlets.SlingSafeMethodsServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.SQLException;

/**
 * @author Marnix Cook
 *
 * This servlet is able to reset the synchronization state table. It will empty it
 * after which a reindex is required. This is only to be invoked in most desparate circumstances.
 */
@SlingServlet(paths = "/bin/contentgraph/reset.do", methods = "GET")
public class ResetSynchStateServlet extends SlingSafeMethodsServlet {

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(ResetSynchStateServlet.class);

    /**
     * Contains the databasse information
     */
    @Reference
    private DatabaseSynchronizer dbSynch;

    /**
     * Status manager
     */
    private SynchronizationManager synchronizationManager = getSynchronizationManager();

    /**
     * Provide a GET method implementation that will reset the synchronisation state
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

            synchronizationManager.reset(db);
            response.getWriter().write(
                    "<html><body>" +
                            "<p>Synchronisation state truncated.</p>" +
                            "<p>" +
                                "<a href=\"/bin/contentgraph/reindex.do\">Please reindex the content immediately by clicking here.</a>" +
                            "</p>" +
                        "</body></html>"
            );
            response.setStatus(HttpServletResponse.SC_OK);

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
     * @return the status manager instance (part of class seam)
     */
    protected SynchronizationManager getSynchronizationManager() {
        return new SynchronizationManager();
    }

}
