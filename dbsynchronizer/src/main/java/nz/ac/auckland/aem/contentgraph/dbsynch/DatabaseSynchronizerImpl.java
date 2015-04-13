package nz.ac.auckland.aem.contentgraph.dbsynch;

import nz.ac.auckland.aem.contentgraph.dbsynch.periodic.PathQueue;
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
    name = "UoA Database Instant Synchronizer"
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

    @Reference
    private PathQueue pathQueue;

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
            LOG.info("Instant synchronization not enabled, stopping synch");
            return;
        }

        LOG.info("Queuing add/update: " + resource.getPath());
        pathQueue.add(resource.getPath());
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

        LOG.info("Queuing delete: " + path);
        pathQueue.delete(path);
    }



    public ConnectionInfo getConnectionInfo() {
        return this.connInfo;
    }
}
