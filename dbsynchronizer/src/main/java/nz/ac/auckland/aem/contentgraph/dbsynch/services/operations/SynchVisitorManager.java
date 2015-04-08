package nz.ac.auckland.aem.contentgraph.dbsynch.services.operations;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.Database;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.visitors.SynchVisitor;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;

/**
 * @author Marnix Cook
 */
public class SynchVisitorManager {


    /**
     * Recursive visit through base
     *
     * @param base is the base path to iterate from
     * @param exclude all the paths that are to be excluded from visting
     * @param visitor the visitor to call the node with
     */
    public void recursiveVisit(Database db, Node base, String[] exclude, SynchVisitor visitor) throws Exception {
        // base cases

        // 1. null?
        if (base == null) {
            return;
        }

        // 2. excluded path?
        if (isExcludedPath(base.getPath(), exclude)) {
            return;
        }

        // execute.
        visitor.visit(db, base);

        // children? recurse.
        if (base.hasNodes()) {
            NodeIterator nIterator = base.getNodes();
            while (nIterator.hasNext()) {
                Node childNode = nIterator.nextNode();
                recursiveVisit(db, childNode, exclude, visitor);
            }
        }
    }

    /**
     * Determine whether the <code>path</code> is excluded
     *
     * @param path is the path to check
     * @param exclude is the list of exclusions
     * @return true if the path is an exclusion path
     * @throws RepositoryException
     */
    protected boolean isExcludedPath(String path, String[] exclude) throws RepositoryException {
        for (String exclPath : exclude) {
            if (path.startsWith(exclPath)) {
                return true;
            }
        }
        return false;
    }


}
