package nz.ac.auckland.aem.contentgraph;

/**
 * @author Marnix Cook
 */
public interface SynchronizationPaths {

    /**
     * @return the paths that are to be included when synchronization happens
     */
    public String[] getIncludePaths();

    /**
     * @return the paths that are to be excluded from synchronisation
     */
    public String[] getExcludedPaths();


}
