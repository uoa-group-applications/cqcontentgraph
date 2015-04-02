package nz.ac.auckland.aem.contentgraph.workflow;

import com.adobe.granite.workflow.exec.WorkflowProcess;

/**
 * @author Marnix Cook
 *
 * Marker interface for synchronization workflow step, inherits
 * the workflow process interface.
 */
public interface SynchWorkflowStep extends WorkflowProcess {

    /**
     * @return the paths that are to be included when synchronization happens
     */
    public String[] getIncludePaths();

    /**
     * @return the paths that are to be excluded from synchronisation
     */
    public String[] getExcludedPaths();

}
