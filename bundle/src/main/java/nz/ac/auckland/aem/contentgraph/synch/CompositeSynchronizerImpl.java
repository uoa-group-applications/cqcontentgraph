package nz.ac.auckland.aem.contentgraph.synch;

import org.apache.felix.scr.annotations.*;
import org.apache.sling.api.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Marnix Cook
 *
 * The composite class
 */
@Service
@Component(immediate = true)
public class CompositeSynchronizerImpl implements CompositeSynchronizer {

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(CompositeSynchronizerImpl.class);

    /**
     * Get a list of synchronizers
     */
    @Reference(
        referenceInterface = Synchronizer.class,
        cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE,
        policy = ReferencePolicy.DYNAMIC,
        bind = "addSynchronizer",
        unbind = "removeSynchronizer"
    )
    private List<Synchronizer> synchronizers = new ArrayList<Synchronizer>();

    // ----------------------------------------------------------------------
    //      Interface implementation
    // ----------------------------------------------------------------------

    /**
     * Call synch on all composite nodes
     *
     * @param resource is the node to synchronize
     */
    @Override
    public void synch(Resource resource) {
        for (Synchronizer synch : this.synchronizers) {
            synch.synch(resource);
        }
    }

    @Override
    public void delete(String path) {
        for (Synchronizer synch : this.synchronizers) {
            synch.delete(path);
        }
    }

    // ----------------------------------------------------------------------
    //      Reference implementation
    // ----------------------------------------------------------------------

    /**
     * Remove the synchronizer
     *
     * @param synch
     */
    public void removeSynchronizer(Synchronizer synch) {
        this.synchronizers.remove(synch);
    }


    /**
     * Add synchronizer to the list
     *
     * @param synch
     */
    public void addSynchronizer(Synchronizer synch) {
        if (synch instanceof CompositeSynchronizer) {
            LOG.info("Not going to add self to composite");
            return;
        }
        this.synchronizers.add(synch);
    }

}
