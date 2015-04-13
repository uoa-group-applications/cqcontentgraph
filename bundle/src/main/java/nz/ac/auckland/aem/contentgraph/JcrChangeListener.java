package nz.ac.auckland.aem.contentgraph;

import org.osgi.service.event.EventHandler;

/**
 * @author Marnix Cook
 */
public interface JcrChangeListener extends EventHandler {

    /**
     * @return the paths that are to be included when synchronization happens
     */
    public String[] getIncludePaths();

    /**
     * @return the paths that are to be excluded from synchronisation
     */
    public String[] getExcludedPaths();

}
