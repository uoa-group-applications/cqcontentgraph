package nz.ac.auckland.aem.contentgraph.dbsynch.services.visitors;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.Database;

/**
 * The visitor is something that is called when the synchronizer needs to
 * perform a specific operation on a node, subtree, or whole content tree.
 */
public interface SynchVisitor<E> {

    /**
     * Called to perform a certain operation on a node.
     *
     * @param db the connection to use
     * @param input the thing to parse
     * @throws Exception
     */
    public void visit(Database db, E input) throws Exception;

}
