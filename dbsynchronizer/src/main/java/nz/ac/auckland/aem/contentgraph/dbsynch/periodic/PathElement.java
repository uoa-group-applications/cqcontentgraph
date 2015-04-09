package nz.ac.auckland.aem.contentgraph.dbsynch.periodic;

/**
 * @author Marnix Cook
 *
 * Path element contains information that gets stored in the PathQueue object
 */
public class PathElement {

    public static enum PathOperation {
        Update,
        Delete;
    }

    private String path;
    private PathOperation op;

    /**
     * Initialize data-members
     *
     * @param path
     * @param op
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
}
