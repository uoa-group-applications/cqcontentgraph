package nz.ac.auckland.aem.contentgraph.dbsynch.services.visitors;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.dao.NodeDAO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dao.PropertyDAO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dto.NodeDTO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dto.PropertyDTO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.Database;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.operations.NodeTransform;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.operations.TransactionManager;
import nz.ac.auckland.aem.contentgraph.utils.PerformanceReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import java.sql.Connection;
import java.sql.SQLException;
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
    private PerformanceReport report = PerformanceReport.getInstance();

    /**
     * Called when a node is visited.
     *
     * @param jcrNode the node to persist
     * @throws javax.jcr.RepositoryException
     */
    public void visit(Database db, Node jcrNode) throws Exception {
        Connection dbConn = db.getConnection();

        LOG.debug("Visiting node to persist: {}", jcrNode.getPath());

        Long start = System.currentTimeMillis();
        NodeDTO nodeDto = trans.getNodeDTO(jcrNode);
        report.addToCategory("getNodeDTO", System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        List<PropertyDTO> propertyDtos = trans.getPropertyDTOList(jcrNode);
        report.addToCategory("getPropertyDTOList", System.currentTimeMillis() - start);

        Long nodeId = insert(db, nodeDto);
        handleProperties(db, nodeDto, propertyDtos, nodeId);
    }

    protected void handleProperties(Database db, NodeDTO node, List<PropertyDTO> propertyDtos, Long nodeId) throws SQLException, RepositoryException {
        Long start;
        start = System.currentTimeMillis();
        propertyDao.removeAll(db, node.getPath(), node.getSub());

        for (PropertyDTO prop : propertyDtos) {
            prop.setNodeId(nodeId);
        }

        propertyDao.insertAll(db, propertyDtos);
        propertyDao.executeBatch(db);
        report.addToCategory("propertyDao.insert", System.currentTimeMillis() - start);
    }


    protected Long insert(Database db, NodeDTO nodeDto) throws SQLException {
        Long start = System.currentTimeMillis();
        Long nodeId = nodeDao.insert(db, nodeDto);
        report.addToCategory("persist.insert", System.currentTimeMillis() - start);
        return nodeId;
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
