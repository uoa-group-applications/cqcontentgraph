package nz.ac.auckland.aem.contentgraph.dbsynch.reindex;

import nz.ac.auckland.aem.contentgraph.dbsynch.DatabaseSynchronizer;
import nz.ac.auckland.aem.contentgraph.dbsynch.DatabaseSynchronizerImpl;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.ConnectionInfo;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.operations.SynchronizationManager;
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
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import java.sql.Connection;
import java.sql.SQLException;

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

    /**
     * Synchronization manager
     */
    private SynchronizationManager sMgr = getSynchronizationManagerInstance();

    /**
     * Synchronize visitor instance
     */
    private SynchVisitor sVisitor = getSynchVisitorInstance();

    /**
     * The runnable method
     */
    @Override
    public void run() {
        LOG.info("Starting to run the reindexer in the background.");

        ConnectionInfo connInfo = this.dbSynch.getConnectionInfo();
        Connection dbConn = null;

        try {
            dbConn = JDBCHelper.getDatabaseConnection(connInfo);
            sMgr.startReindex(dbConn);

            // iterate over all base paths
            for (String includePath : this.getIncludePaths()) {
                Resource inclResource = this.getResourceResolver().getResource(includePath);
                if (inclResource == null) {
                    LOG.error("Could not find `{}`, skipping", includePath);
                }

                // node
                Node inclNode = inclResource.adaptTo(Node.class);

                // recurse
                recursiveVisit(inclNode, this.getExcludePaths(), this.sVisitor);
            }

            sMgr.finished(dbConn);

            LOG.info("Successfully finished the re-indexing process");
        }
        catch (Exception ex) {
            LOG.error("Something went wrong during the reindexing process. Finished with errors.", ex);
            writeErrorMessage(dbConn, ex);
        }
        finally {
            JDBCHelper.closeQuietly(dbConn);
        }

    }

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
     * Recursive visit through base
     *
     * @param base is the base path to iterate from
     * @param exclude all the paths that are to be excluded from visting
     * @param visitor the visitor to call the node with
     */
    protected void recursiveVisit(Node base, String[] exclude, SynchVisitor visitor) throws RepositoryException {
        // base cases

        // 1. null?
        if (base == null) {
            return;
        }

        // 2. excluded path?
        if (isExcludedPath(base.getPath(), exclude)) {
            return;
        }

        // execute.
        visitor.visit(base);

        // children? recurse.
        if (base.hasNodes()) {
            NodeIterator nIterator = base.getNodes();
            while (nIterator.hasNext()) {
                Node childNode = nIterator.nextNode();
                recursiveVisit(childNode, exclude, visitor);
            }
        }
    }

    /**
     * Determine whether the <code>path</code> is excluded
     *
     * @param path is the path to check
     * @param exclude is the list of exclusions
     * @return true if the path is an exclusion path
     * @throws RepositoryException
     */
    protected boolean isExcludedPath(String path, String[] exclude) throws RepositoryException {
        for (String exclPath : exclude) {
            if (path.startsWith(exclPath)) {
                return true;
            }
        }
        return false;
    }


    /**
     * TODO: Read from bundle configuration
     *
     * @return all the paths that should be read
     */
    protected String[] getIncludePaths() {
        return new String[] {
            "/content",
            "/etc/tags"
        };
    }

    /**
     * TODO: Read from bundle configuration
     *
     * @return the paths that should not be read
     */
    protected String[] getExcludePaths() {
        return new String[] {
            "/content/usergenerated"
        };
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

    protected SynchVisitor getSynchVisitorInstance() {
        return new SynchVisitor();
    }
}
