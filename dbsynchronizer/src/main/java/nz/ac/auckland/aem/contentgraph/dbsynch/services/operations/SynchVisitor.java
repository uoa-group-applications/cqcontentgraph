package nz.ac.auckland.aem.contentgraph.dbsynch.services.operations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import javax.jcr.RepositoryException;

/**
 * Code that is called for each node.
 */
public class SynchVisitor {

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(SynchVisitor.class);

    /**
     * Called when a node is visited.
     *
     * @param node
     * @throws javax.jcr.RepositoryException
     */
    public void visit(Node node) throws RepositoryException {
        LOG.info("Visiting node: {}", node.getPath());
    }
}
