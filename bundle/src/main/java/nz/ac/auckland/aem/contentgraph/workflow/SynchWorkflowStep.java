package nz.ac.auckland.aem.contentgraph.workflow;

import com.adobe.granite.workflow.WorkflowException;
import com.adobe.granite.workflow.WorkflowSession;
import com.adobe.granite.workflow.exec.WorkItem;
import com.adobe.granite.workflow.exec.WorkflowData;
import com.adobe.granite.workflow.exec.WorkflowProcess;
import com.adobe.granite.workflow.metadata.MetaDataMap;
import nz.ac.auckland.aem.contentgraph.synch.CompositeSynchronizer;
import nz.ac.auckland.aem.contentgraph.utils.ValidPathHelper;
import org.apache.felix.scr.annotations.*;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

/**
 * @author Marnix Cook
 *
 * This class is called as part of a synchronisation step on the publication
 * server. Its launcher configuration can be found in the content package.
 */
@Service
@Component(
    metatype = true,
    immediate = true,
    name = "UoA Synchronize Content Workflow Step",
    description = "Used to synchronize content between JCR and RDBMS"
)
@Properties({
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
public class SynchWorkflowStep implements WorkflowProcess {

    @Reference
    private CompositeSynchronizer compositeSync;

    @Reference
    private ResourceResolverFactory resourceResolverFactory;

    /**
     * Include paths
     */
    private String[] includePaths;

    /**
     * Exclude paths
     */
    private String[] excludePaths;

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(SynchWorkflowStep.class);

    @Activate @Modified
    public void activate(ComponentContext context) {
        this.includePaths = (String[]) context.getProperties().get("include");
        this.excludePaths = (String[]) context.getProperties().get("exclude");
    }

    /**
     * Execute the workflow
     *
     * @param workItem
     * @param workflowSession
     * @param metaDataMap
     * @throws WorkflowException
     */
    @Override
    public void execute(WorkItem workItem, WorkflowSession workflowSession, MetaDataMap metaDataMap) throws WorkflowException {

        WorkflowData workflowData = workItem.getWorkflowData();

        if (!isValidPayload(workflowData)) {
            LOG.info("Invalid payload type, aborting");
            return;
        }

        ResourceResolver resolver = null;
        try {
            String path = workflowData.getPayload().toString();
            if (!ValidPathHelper.isTrackedContentPath(path, includePaths, excludePaths)) {
                LOG.info("`{}` is not a tracked path", path);
                return;
            }

            // get resource
            resolver = resourceResolverFactory.getAdministrativeResourceResolver(null);
            Resource resource = resolver.getResource(path);

            // apparently this is a deletion
            if (resource == null) {
                this.compositeSync.delete(path);
            }
            // add or edit, synch
            else {
                this.compositeSync.synch(resource);
            }
        }
        catch (LoginException lEx) {
            LOG.error("Unable to login to get resource resolver", lEx);
        }
        finally {
            if (resolver != null) {
                resolver.close();
            }
        }
    }

    protected boolean isValidPayload(WorkflowData workflowData) {
        return "JCR_PATH".equals(workflowData.getPayloadType());
    }
}
