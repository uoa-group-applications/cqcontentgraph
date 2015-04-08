package nz.ac.auckland.aem.contentgraph.dbsynch.services.visitors;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.dao.NodeDAO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dao.PropertyDAO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dto.NodeDTO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dto.PropertyDTO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.Database;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.operations.NodeTransform;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.operations.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import java.sql.Connection;
import java.util.List;

/**
 * @author Marnix Cook
 *
 * Code that is called for each node that needs to be persisted to the database.
 */
public class PersistSynchVisitor implements SynchVisitor<Node> {

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(SynchVisitor.class);

    // ------------------------------------------------------------
    //   Class dependencies
    // ------------------------------------------------------------

    private TransactionManager txMgr = getTransactionManager();
    private NodeDAO nodeDao = getNodeDAOInstance();
    private PropertyDAO propertyDao = getPropertyDAOInstance();
    private NodeTransform trans = getNodeTransformInstance();

    /**
     * Called when a node is visited.
     *
     * @param jcrNode the node to persist
     * @throws javax.jcr.RepositoryException
     */
    public void visit(Database db, Node jcrNode) throws Exception {
        Connection dbConn = db.getConnection();
        LOG.info("Visiting node to persist: {}", jcrNode.getPath());

        NodeDTO nodeDto = trans.getNodeDTO(jcrNode);
        List<PropertyDTO> propertyDtos = trans.getPropertyDTOList(jcrNode);

        Long nodeId = nodeDao.insert(db, nodeDto);

        propertyDao.removeAll(db, jcrNode.getPath());

        for (PropertyDTO prop : propertyDtos) {
            prop.setNodeId(nodeId);
            propertyDao.insert(db, prop);
        }

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
