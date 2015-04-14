package nz.ac.auckland.aem.contentgraph.dbsynch.services.operations;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.Database;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.visitors.SynchVisitor;
import nz.ac.auckland.aem.contentgraph.utils.PerformanceReport;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import java.sql.SQLException;

/**
 * @author Marnix Cook
 */
public class SynchVisitorManager {

    public static final int COMMIT_THRESHOLD = 2048;
    private int batchCount;

    /**
     * Reset the batch count
     */
    public void reset() {
        this.batchCount = 0;
    }

    /**
     * Recursive visit through base
     *
     * @param base is the base path to iterate from
     * @param exclude all the paths that are to be excluded from visting
     * @param visitor the visitor to call the node with
     */
    public int recursiveVisit(Database db, Node base, String[] exclude, SynchVisitor visitor) throws Exception {
        int nVisits = 0;
        // base cases

        // 1. null?
        if (base == null) {
            return 0;
        }

        // 2. excluded path?
        if (isExcludedPath(base.getPath(), exclude)) {
            return 0;
        }

        // execute.
        visitor.visit(db, base);

        ++nVisits;

        Long start = System.currentTimeMillis();

        // make sure to commit when necessary
        commitOnThreshold(db);

        PerformanceReport.getInstance().addToCategory(
                "commitOnThreshold", System.currentTimeMillis() - start
            );

        // children? recurse.
        if (base.hasNodes()) {
            NodeIterator nIterator = base.getNodes();
            while (nIterator.hasNext()) {
                Node childNode = nIterator.nextNode();
                nVisits += recursiveVisit(db, childNode, exclude, visitor);
            }
        }

        return nVisits;

    }

    /**
     * Make sure the transaction is committed every `commit_threshold` nodes
     * that have been inserted.
     *
     * @param db
     * @throws SQLException
     */
    protected void commitOnThreshold(Database db) throws SQLException {
        ++this.batchCount;
        if (this.batchCount > COMMIT_THRESHOLD){
            this.batchCount %= COMMIT_THRESHOLD;
            db.getConnection().commit();
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
