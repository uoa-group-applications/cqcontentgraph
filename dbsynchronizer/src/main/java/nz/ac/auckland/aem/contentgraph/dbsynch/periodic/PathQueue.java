package nz.ac.auckland.aem.contentgraph.dbsynch.periodic;

import java.util.Set;

/**
 * @author Marnix Cook
 *
 * The path queue will contain paths that have been signalled as updated and
 * will be emptied once the periodic update job runs.
 *
 * The methods in its implementation should be thread-safe.
 */
public interface PathQueue {

    /**
     * Add a path to be synchronized
     *
     * @param path the path to add
     */
    public void add(String path);

    /**
     * Add a path to be deleted
     *
     * @param path to be deleted
     */
    public void delete(String path);

    /**
     * Flushes the queue and returns the current set of paths that
     * have been added.
     *
     * @return the paths.
     */
    public Set<PathElement> flushAndGet();

}
