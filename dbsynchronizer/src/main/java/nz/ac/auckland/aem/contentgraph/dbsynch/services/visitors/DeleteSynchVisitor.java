package nz.ac.auckland.aem.contentgraph.dbsynch.services.visitors;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.dao.NodeDAO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dao.PropertyDAO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.Database;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.operations.NodeTransform;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.operations.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

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
    public void visit(Database db, String path) throws Exception {
        Connection dbConn = db.getConnection();

        LOG.info("Deleting from database: " + path);

        txMgr.start(dbConn);

        nodeDao.removeAll(db, path);
        propertyDao.removeAll(db, path);

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
