package nz.ac.auckland.aem.contentgraph.dbsynch.periodic;

import static nz.ac.auckland.aem.contentgraph.dbsynch.periodic.PathElement.PathOperation.Delete;

/**
 * @author Marnix Cook
 *
 * Path element contains information that gets stored in the PathQueue object
 */
public class PathElement implements Comparable<PathElement> {

    public static enum PathOperation {
        Update,
        Delete
    }

    private String path;
    private PathOperation op;

    /**
     * Initialize data-members
     *
     * @param path the path of the element
     * @param op the operation that has to happen
     */
    public PathElement(String path, PathOperation op) {
        this.path = path;
        this.op = op;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public PathOperation getOp() {
        return op;
    }

    public void setOp(PathOperation op) {
        this.op = op;
    }


    public boolean equals(PathElement other) {
        return this.path.equals(other.path) && this.op == other.op;
    }

    @Override
    public int compareTo(PathElement other) {

        // if it's the same path, make sure the delete operation
        // has priority over an update operation
        if (this.path.equals(other.path)) {
            if (this.op == other.op) {
                return 0;
            } else if (this.op == Delete) {
                return -1;
            } else {
                return 1;
            }
        }

        // fallback to path comparison
        return this.path.compareTo(other.path);
    }
}
