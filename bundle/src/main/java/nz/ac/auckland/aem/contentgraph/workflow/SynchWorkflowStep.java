package nz.ac.auckland.aem.contentgraph.workflow;

import com.adobe.granite.workflow.WorkflowException;
import com.adobe.granite.workflow.WorkflowSession;
import com.adobe.granite.workflow.exec.WorkItem;
import com.adobe.granite.workflow.exec.WorkflowData;
import com.adobe.granite.workflow.exec.WorkflowProcess;
import com.adobe.granite.workflow.metadata.MetaDataMap;
import nz.ac.auckland.aem.contentgraph.synch.CompositeSynchronizer;
import nz.ac.auckland.aem.contentgraph.utils.ValidPathHelper;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
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
@Component(immediate = true)
public class SynchWorkflowStep implements WorkflowProcess {

    @Reference
    private CompositeSynchronizer compositeSync;

    @Reference
    private ResourceResolverFactory resourceResolverFactory;

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(SynchWorkflowStep.class);

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
            if (!ValidPathHelper.isTrackedContentPath(path, getArguments(metaDataMap))) {
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
        catch (RepositoryException rEx) {
            LOG.error("Something went wrong in the repository", rEx);
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



    private String getArguments(MetaDataMap metaDataMap) throws RepositoryException {
        return ((Value) metaDataMap.get("PROCESS_ARGS")).getString();
    }

    protected boolean isValidPayload(WorkflowData workflowData) {
        return "JCR_PATH".equals(workflowData.getPayloadType());
    }
}
