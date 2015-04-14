package nz.ac.auckland.aem.contentgraph.dbsynch.services.visitors;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.dao.NodeDAO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dao.PropertyDAO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dto.NodeDTO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dto.PropertyDTO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.Database;
import nz.ac.auckland.aem.contentgraph.utils.PerformanceReport;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import java.sql.SQLException;
import java.util.List;

/**
 * @author Marnix Cook
 *
 * Reindex version of the persist synch visitor which uses the 'forceInsert' method.
 */
public class ReindexPersistSynchVisitor extends PersistSynchVisitor {

    /**
     * Node DAO instance
     */
    private NodeDAO nodeDao = getNodeDAOInstance();

    /**
     * Property DAO instance
     */
    private PropertyDAO propertyDao = getPropertyDAOInstance();


    /**
     * Override the insert method so that we can use the `forceInsert` method.
     *
     * @param db is the database
     * @param nodeDto is the node to use
     * @return the newly inserted ID
     *
     * @throws SQLException
     */
    @Override
    protected Long insert(Database db, NodeDTO nodeDto) throws SQLException {
        Long start = System.currentTimeMillis();
        Long nodeId = nodeDao.forceInsert(db, nodeDto);

        PerformanceReport
                .getInstance()
                .addToCategory("persist.insert", System.currentTimeMillis() - start);

        return nodeId;
    }

    /**
     * Override the handle properties method because we know that there are no existing
     * properties
     *
     * @param db database to write to
     * @param jcrNode is the jcr node to write the properties for
     * @param propertyDtos is the list of properties
     * @param nodeId the node identifier to attach them to
     *
     * @throws SQLException
     * @throws RepositoryException
     */
    @Override
    protected void handleProperties(Database db, Node jcrNode, List<PropertyDTO> propertyDtos, Long nodeId) throws SQLException, RepositoryException {
        Long start = System.currentTimeMillis();

        for (PropertyDTO prop : propertyDtos) {
            prop.setNodeId(nodeId);
        }

        propertyDao.insertAll(db, propertyDtos);

        PerformanceReport
                .getInstance()
                .addToCategory("propertyDao.insert", System.currentTimeMillis() - start);
    }

}
