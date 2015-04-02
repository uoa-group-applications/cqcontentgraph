package nz.ac.auckland.aem.contentgraph.dbsynch.services.visitors;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.dao.NodeDAO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dao.PropertyDAO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dto.NodeDTO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dto.PropertyDTO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.operations.NodeTransform;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.operations.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * The visitor is something that is called when the synchronizer needs to
 * perform a specific operation on a node, subtree, or whole content tree.
 */
public interface SynchVisitor<E> {

    /**
     * Called to perform a certain operation on a node.
     *
     * @param dbConn the connection to use
     * @param input the thing to parse
     * @throws Exception
     */
    public void visit(Connection dbConn, E input) throws Exception;

}
