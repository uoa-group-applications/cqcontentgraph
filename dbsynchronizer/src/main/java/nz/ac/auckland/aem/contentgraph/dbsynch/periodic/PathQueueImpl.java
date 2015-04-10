package nz.ac.auckland.aem.contentgraph.dbsynch.periodic;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

import static nz.ac.auckland.aem.contentgraph.dbsynch.periodic.PathElement.PathOperation.*;

/**
 * @author Marnix Cook
 */
@Service
@Component
public class PathQueueImpl implements PathQueue {

    private Set<PathElement> paths;

    /**
     * Initialize data-members
     */
    public PathQueueImpl() {
        reset();
    }

    /**
     * Path to add
     *
     * @param path the path to add
     */
    @Override
    public synchronized void add(String path) {
        this.paths.add(new PathElement(path, Update));
    }

    @Override
    public void delete(String path) {
        this.paths.add(new PathElement(path, Delete));
    }

    /**
     * Flush and retrieve the path contents
     *
     * @return the current set of paths
     */
    @Override
    public synchronized Set<PathElement> flushAndGet() {
        Set<PathElement> oldPaths = this.paths;
        reset();
        return oldPaths;
    }

    /**
     * Reset the queue
     */
    protected void reset() {
        this.paths = new TreeSet<PathElement>();
    }
}
