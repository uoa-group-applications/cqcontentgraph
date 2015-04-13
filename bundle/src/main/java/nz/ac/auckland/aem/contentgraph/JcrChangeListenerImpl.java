package nz.ac.auckland.aem.contentgraph;

import nz.ac.auckland.aem.contentgraph.synch.CompositeSynchronizer;
import org.apache.commons.lang.StringUtils;
import org.apache.felix.scr.annotations.*;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.event.jobs.JobProcessor;
import org.apache.sling.event.jobs.JobUtil;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Marnix Cook
 *
 * The class listens to JCR events and passes them on to the collection of
 * synchronizers that have been registered inside the composite
 * synchronizer.
 */
@Service(JcrChangeListener.class)
@Component(
        immediate = true,
        metatype = true,
        name = "UoA JCR Change Listener"
)
@Properties({
    @Property(
        name = "event.topics",
        value = "org/apache/sling/api/resource/Resource/*"
    ),
    @Property(
        name = "include",
        label = "Include these paths",
        description = "Paths that are included in synchronization (eg. /content/, /etc/tags)",
        cardinality = Integer.MAX_VALUE
    ),
    @Property(
        name = "exclude",
        label = "Exclude these paths",
        description = "Paths that are excluded from synchronization (eg. /content/usergenerated, /content/catalog)",
        cardinality = Integer.MAX_VALUE
    )
})
public class JcrChangeListenerImpl implements JcrChangeListener, JobProcessor {

    /**
     * Word constant for added clarity
     */
    public static final boolean SUCCESSFUL = true;

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(JcrChangeListenerImpl.class);

    /**
     * A collection of synchronizers
     */
    @Reference
    private CompositeSynchronizer synchronizers;

    /**
     * Resource resolver factory instance bound here
     */
    @Reference
    private ResourceResolverFactory resourceResolverFactory;

    /**
     * Resource resolver instance
     */
    private ResourceResolver resourceResolver;

    /**
     * Include paths
     */
    private String[] includePaths;

    /**
     * Exclude paths
     */
    private String[] excludePaths;

    /**
     * Called when the bundle is either activated
     *
     * @param context is the component context
     */
    @Activate
    public void activation(ComponentContext context) {

        this.includePaths = (String[]) context.getProperties().get("include");
        this.excludePaths = (String[]) context.getProperties().get("exclude");

        if (this.resourceResolver != null) {
            LOG.info("Resource resolver already instantiated, skipping activation code");
            return;
        }

        try {
            this.resourceResolver = this.resourceResolverFactory.getAdministrativeResourceResolver(null);
        }
        catch (LoginException lEx) {
            LOG.error("Cannot generate a resource resolver for the JcrChangeListener");
        }
    }

    /**
     * Called when the bundle is deactivated
     */
    @Deactivate
    public void deactivation() {
        if (resourceResolver != null && resourceResolver.isLive()) {
            resourceResolver.close();
        }
    }


    /**
     * Called when a JCR event happens, will schedule the job. Event handlers
     * that take too long could be blacklisted, that's why we're doing that.
     *
     * @param event the event that happened.
     */
    public void handleEvent(Event event) {
        JobUtil.processJob(event, this);
    }

    /**
     * Handles the incoming job in a separate thread
     */
    @Override
    public boolean process(Event job) {

        String path = (String) job.getProperty("path");
        if (StringUtils.isBlank(path)) {
            LOG.info("The job path was empty, skipping");
            return SUCCESSFUL;
        }

        if (isDeletion(job)) {
            LOG.debug("Node `{}` being deleted", path);
            synchronizers.delete(path);
        }
        else if (isMutation(job)) {
            LOG.debug("Node `{}` being mutated", path);
            Resource pathResource = this.resourceResolver.getResource(path);
            synchronizers.synch(pathResource);
        }


        return SUCCESSFUL;
    }

    /**
     * @return the excluded paths
     */
    public String[] getExcludedPaths() {
        return this.excludePaths;
    }

    /**
     * @return the included paths
     */
    public String[] getIncludePaths() {
        return this.includePaths;
    }


    protected boolean isMutation(Event job) {
        return job.getTopic().endsWith("ADDED") || job.getTopic().endsWith("CHANGED");
    }

    protected boolean isDeletion(Event job) {
        return job.getTopic().endsWith("REMOVED");
    }
}
