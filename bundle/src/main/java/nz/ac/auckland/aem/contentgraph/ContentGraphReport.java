package nz.ac.auckland.aem.contentgraph;

import com.day.cq.wcm.api.Page;

import nz.ac.auckland.aem.contentgraph.utils.HttpContext;
import nz.ac.auckland.aem.contentgraph.writer.ContentWriter;
import nz.ac.auckland.aem.contentgraph.writer.ContentWriterFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.felix.scr.annotations.*;
import org.apache.felix.scr.annotations.sling.SlingServlet;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.api.servlets.SlingSafeMethodsServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.*;
import javax.servlet.ServletException;
import java.io.IOException;
import java.util.*;

import static nz.ac.auckland.aem.contentgraph.ChildIteratorType.DontVisitChildren;
import static nz.ac.auckland.aem.contentgraph.ChildIteratorType.VisitChildren;

/**
 * @author Marnix Cook
 *
 * Outputs a format that is able to generate a content graph report
 */
@SlingServlet(paths = "/bin/contentgraph.do", methods = "GET")
public class ContentGraphReport extends SlingSafeMethodsServlet {

    public static final String TCL = "tcl";
    public static final String PARAM_OUTPUT = "output";
    public static final String PARAM_PATHS = "paths";
    public static final String PARAM_EXCLUDE = "exclude";
    public static final String TYPE_PAGE = "cq:Page";
    public static final String TYPE_ASSET = "dam:Asset";
    /**
     * Http context interface that allows us to write.
     */
    @Reference
    private HttpContext context;

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(ContentGraphReport.class);

    /**
     * Resolver factory instance bound here.
     */
    @Reference
    private ResourceResolverFactory rrFactory;


    /**
     * Initialize the query
     */
    public ContentGraphReport() {
    }

    @Override
    protected void doGet(SlingHttpServletRequest request, SlingHttpServletResponse response) throws ServletException, IOException {
        GraphReportConfig reportConfig = getReportConfigInstance(request);
        if (reportConfig == null) {
            return;
        }

        this.context.setContext(request, response);

        reportConfig.getWriter().showHeader();

        ResourceResolver resolver = null;
        try {
            resolver = rrFactory.getAdministrativeResourceResolver(null);

            // iterate over the base paths
            for (String basePath : reportConfig.getPaths()) {
                parseTreeFrom(reportConfig, resolver, basePath);
            }

        }
        catch (LoginException lEx) {
            LOG.error("Could not get administrative resource resolver", lEx);
        }
        finally {
            if (resolver != null) {
                resolver.close();
            }
        }

        reportConfig.getWriter().showFooter();

        this.context.getResponse().flushBuffer();
    }



    /**
     * Parse a tree node by node
     *
     * @param reportConfig
     * @param resolver
     * @param basePath
     */
    protected void parseTreeFrom(GraphReportConfig reportConfig, ResourceResolver resolver, String basePath) {
        Resource baseResource = resolver.getResource(basePath);
        if (baseResource == null) {
            LOG.error("Could not find `{}` resource", basePath);
            return;
        }

        try {
            visitNode(reportConfig, baseResource);
        }
        catch (RepositoryException rEx) {
            LOG.error("An error occurred", rEx);
        }
    }


    /**
     * Parse `node` and iterate over children if they exist for <code>node</code>
     *
     * @param reportConfig
     * @param resource is the node to visit the children on
     * @throws RepositoryException
     */
    protected void visitNode(GraphReportConfig reportConfig, Resource resource) throws RepositoryException {

        // can parse this?
        if (reportConfig.isExcludedPath(resource.getPath())) {
            return;
        }

        // do something with the resource
        ChildIteratorType ciType = parseResource(reportConfig, resource);

        if (ciType == VisitChildren) {

            // has children? iterate over them.
            Iterator<Resource> childIterator = resource.getChildren().iterator();
            while (childIterator.hasNext()) {
                visitNode(reportConfig, childIterator.next());
            }

        }
    }


    /**
     * Parse node information
     *
     * @param reportConfig
     * @param resource
     * @throws RepositoryException
     */
    private ChildIteratorType parseResource(GraphReportConfig reportConfig, Resource resource) throws RepositoryException {

        ContentWriter writer = reportConfig.getWriter();
        Node node = resource.adaptTo(Node.class);

        if (isPage(node)) {

            // get page
            Page page = getPageForNode(node);

            writer.writeNode(node, page.getTitle());
            writer.writeProperties(node);

        } else {

            writer.writeNode(node, node.getName());
            writer.writeProperties(node);

        }

        return isAsset(node) ? DontVisitChildren : VisitChildren;
    }

    protected Page getPageForNode(Node node) throws RepositoryException {
        return context.getRequest().getResourceResolver().getResource(node.getPath()).adaptTo(Page.class);
    }

    /**
     * @return true if this is a proper page?
     */
    protected boolean isPage(Node node) throws RepositoryException {
        return node.getPrimaryNodeType().getName().equals(TYPE_PAGE);
    }

    /**
     * @return if the node is an asset
     */
    protected boolean isAsset(Node node) throws RepositoryException {
        return node.getPrimaryNodeType().getName().equals(TYPE_ASSET);
    }

    /**
     * Setup the graph report configuration
     *
     * @param request is the request to get parameter values from
     * @return a report configuration instance
     */
    protected GraphReportConfig getReportConfigInstance(SlingHttpServletRequest request) {

        if (StringUtils.isBlank(request.getParameter(PARAM_PATHS))) {
            this.context.println("Error: Please specify the `paths` parameter with comma separated content-paths");
            return null;
        }

        String[] basePaths = request.getParameter(PARAM_PATHS).split(",");
        String[] exclusions =
                StringUtils.isNotBlank(request.getParameter(PARAM_EXCLUDE))
                        ? request.getParameter(PARAM_EXCLUDE).split(",")
                        : new String[0];

        return
            new GraphReportConfig(
                    basePaths,
                    exclusions,
                    getContentWriterInstance(request)
            );
    }

    /**
     * @return the chosen content writer instance, default is 'tcl'
     */
    protected ContentWriter getContentWriterInstance(SlingHttpServletRequest request) {
        String outputType = request.getParameter(PARAM_OUTPUT);
        if (StringUtils.isBlank(outputType)) {
            outputType = TCL;
        }
        return ContentWriterFactory.create(context, outputType);
    }

}
