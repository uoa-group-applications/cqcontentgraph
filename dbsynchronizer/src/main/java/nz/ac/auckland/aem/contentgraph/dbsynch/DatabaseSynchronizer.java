package nz.ac.auckland.aem.contentgraph.dbsynch;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.dao.NodeDAO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dao.PropertyDAO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dto.NodeDTO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dto.PropertyDTO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.ConnectionInfo;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.operations.NodeTransform;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.operations.TransactionManager;
import nz.ac.auckland.aem.contentgraph.synch.Synchronizer;
import org.apache.felix.scr.annotations.*;
import org.apache.sling.api.resource.Resource;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import java.sql.*;
import java.util.List;

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
public class DatabaseSynchronizer implements Synchronizer {

    // -----------------------------------------------------------------
    //      Bundle parameter constants
    // -----------------------------------------------------------------

    public static final String BUNDLE_PARAM_ENABLED = "enabled";
    public static final String BUNDLE_PARAM_JDBC = "jdbc";
    public static final String BUNDLE_PARAM_USER = "user";
    public static final String BUNDLE_PARAM_PASSWORD = "password";

    private NodeDAO nodeDao = getNodeDAOInstance();
    private PropertyDAO propertyDao = getPropertyDAOInstance();
    private NodeTransform trans = getNodeTransformInstance();
    private TransactionManager txMgr = getTxMgrInstance();


    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseSynchronizer.class);

    /**
     * Enabled
     */
    private boolean enabled;

    /**
     * Connection information
     */
    private ConnectionInfo connInfo;

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

        Connection conn = null;

        try {
            conn = getDatabaseConnection(this.connInfo);

            NodeDTO nodeDto = trans.getNodeDTO(jcrNode);
            List<PropertyDTO> propertyDtos = trans.getPropertyDTOList(jcrNode);

            txMgr.start(conn);

            Long nodeId = nodeDao.insert(conn, nodeDto);

            for (PropertyDTO prop : propertyDtos) {
                prop.setNodeId(nodeId);
                propertyDao.insert(conn, prop);
            }

            txMgr.commit(conn);
        }
        catch (SQLException sqlEx) {
            rollback(conn);
        }
        finally {
            closeQuietly(conn);
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

        LOG.info("Deleting from database: " + path);

        Connection conn = null;

        try {
            conn = getDatabaseConnection(this.connInfo);

            txMgr.start(conn);
            nodeDao.removeAll(conn, path);
            propertyDao.removeAll(conn, path);
            txMgr.commit(conn);
        }
        catch (SQLException sqlEx) {
            rollback(conn);
        }
        finally {
            closeQuietly(conn);
        }
    }

    /**
     * Rollback the connection
     *
     * @param conn is the connection instance
     */
    protected void rollback(Connection conn) {
        if (conn != null) {
            if (!txMgr.rollback(conn)) {
                LOG.error("Rollback failed!");
            }
        } else {
            LOG.info("Cannot rollback, connection was already closed");
        }
    }

    // ----------------------------------------------------------------------------------------
    //      Getters for instances that might define the seam of this class
    // ----------------------------------------------------------------------------------------

    protected TransactionManager getTxMgrInstance() {
        return new TransactionManager();
    }

    protected NodeTransform getNodeTransformInstance() {
        return new NodeTransform();
    }

    protected PropertyDAO getPropertyDAOInstance() {
        return new PropertyDAO();
    }

    protected NodeDAO getNodeDAOInstance() {
        return new NodeDAO();
    }


}
