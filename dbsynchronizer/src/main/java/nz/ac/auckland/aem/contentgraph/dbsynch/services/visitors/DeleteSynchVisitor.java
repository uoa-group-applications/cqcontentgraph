package nz.ac.auckland.aem.contentgraph.dbsynch.services.visitors;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.dao.NodeDAO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dao.PropertyDAO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.operations.NodeTransform;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.operations.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import java.sql.Connection;
import java.sql.SQLException;

import static nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper.closeQuietly;
import static nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper.getDatabaseConnection;

/**
 * @author Marnix Cook
 *
 * Visitor that deletes nodes that it is passed in the relation database.
 */
public class DeleteSynchVisitor implements SynchVisitor<String> {

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(DeleteSynchVisitor.class);

    // ------------------------------------------------------------
    //   Class dependencies
    // ------------------------------------------------------------

    private TransactionManager txMgr = getTransactionManager();
    private NodeDAO nodeDao = getNodeDAOInstance();
    private PropertyDAO propertyDao = getPropertyDAOInstance();
    private NodeTransform trans = getNodeTransformInstance();

    @Override
    public void visit(Connection dbConn, String path) throws Exception {
        LOG.info("Deleting from database: " + path);

        txMgr.start(dbConn);

        nodeDao.removeAll(dbConn, path);
        propertyDao.removeAll(dbConn, path);

        txMgr.commit(dbConn);
    }


    // ------------------------------------------------------------
    //   Class seam providers
    // ------------------------------------------------------------

    protected TransactionManager getTransactionManager() {
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
