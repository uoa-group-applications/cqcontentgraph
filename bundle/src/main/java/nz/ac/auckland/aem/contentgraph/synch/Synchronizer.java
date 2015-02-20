package nz.ac.auckland.aem.contentgraph.synch;

import org.apache.sling.api.resource.Resource;

import javax.jcr.Node;

/**
 * @author Marnix Cook
 */
public interface Synchronizer {

    /**
     * Resource is to be synchronized
     *
     * @param resource
     */
    public void synch(Resource resource);

    /**
     * Path is to be deleted
     *
     * @param path
     */
    public void delete(String path);

}
