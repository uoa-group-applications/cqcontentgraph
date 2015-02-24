package nz.ac.auckland.aem.contentgraph.dbsynch;

import nz.ac.auckland.aem.contentgraph.synch.Synchronizer;
import org.apache.felix.scr.annotations.*;
import org.apache.sling.api.resource.Resource;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Marnix Cook
 *
 * This class is an implementation of the synchronizer interface and is invoked
 * whenever a page is published to the publication servers. When the page is published
 * certain actions can be taken. In this case, we'll be updating a database so
 * we can do cool queries on the new content.
 */
@Service
@Component(
    immediate = true,
    metatype = true,
    name = "UoA Database Content Synchronizer"
)
@Properties({
    @Property(
        name = "enabled",
        label = "Enabled",
        description = "Ticked if the database synchronizer is enabled",
        boolValue = true
    )
})
public class DatabaseSynchronizer implements Synchronizer {

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseSynchronizer.class);

    /**
     * Enabled
     */
    private boolean enabled;

    /**
     * Called when the configuration changed
     *
     * @param context
     */
    @Activate @Modified
    public void configChanged(ComponentContext context) {
        this.enabled = (Boolean) context.getProperties().get("enabled");
    }

    /**
     * Called when a page is created or updated
     *
     * @param resource the resource that has been changed
     */
    @Override
    public void synch(Resource resource) {
        if (!this.enabled) {
            LOG.info("Database synchronization not enabled, stopping synch");
            return;
        }

        LOG.info("Synching the database for resource: " + resource.getPath());
    }

    /**
     * Called when a page or asset has been deleted.
     *
     * @param path the path that is no more
     */
    @Override
    public void delete(String path) {
        if (!this.enabled) {
            LOG.info("Database synchronization not enabled, stopping synch");
            return;
        }

        LOG.info("Deleting from database: " + path);
    }

}
