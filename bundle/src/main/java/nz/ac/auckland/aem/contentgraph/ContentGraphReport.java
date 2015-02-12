package nz.ac.auckland.aem.contentgraph;

import com.day.cq.wcm.api.Page;
import nz.ac.auckland.aem.utils.HttpContext;
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
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author Marnix Cook
 *
 * Outputs a format that is able to generate a content graph report
 */
@SlingServlet(paths = "/bin/contentgraph.do", methods = "GET")
public class ContentGraphReport extends SlingSafeMethodsServlet {

    /**
     * Http context interface that allows us to write.
     */
    @Reference
    private HttpContext context;

    private SimpleDateFormat sdFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm");

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
        this.context.setContext(request, response);

        if (StringUtils.isBlank(request.getParameter("paths"))) {
            this.context.println("Error: Please specify the `paths` parameter with comma separated content-paths");
            this.context.getResponse().flushBuffer();
            return;
        }

        showHeader();

        ResourceResolver resolver = null;
        try {
            resolver = rrFactory.getAdministrativeResourceResolver(null);

            Node node = null;
            String[] basePaths = request.getParameter("paths").split(",");

            // iterate over the base paths
            for (String basePath : basePaths) {
                parseTreeFrom(resolver, basePath);
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

        showFooter();

    }

    protected void showFooter() {
        context.println("\nDONE");
    }

    /**
     * Parse a tree node by node
     *
     * @param resolver
     * @param basePath
     */
    protected void parseTreeFrom(ResourceResolver resolver, String basePath) {
        Resource baseResource = resolver.getResource(basePath);
        if (baseResource == null) {
            LOG.error("Could not find `{}` resource", basePath);
            return;
        }

        try {
            visitNode(baseResource);
        }
        catch (RepositoryException rEx) {
            LOG.error("An error occurred", rEx);
        }
    }


    /**
     * Run the parse operation on this node, and then recurse on its children
     *
     * @param resource is the node to visit
     * @throws RepositoryException
     */
    protected void visitNode(Resource resource) throws RepositoryException {
        parseResource(resource);
        visitChildNodes(resource);
    }


    /**
     * Iterate over children if they exist for <code>node</code>
     *
     * @param resource is the node to visit the children on
     * @throws RepositoryException
     */
    private void visitChildNodes(Resource resource) throws RepositoryException {
        Iterable<Resource> childIteratable = resource.getChildren();
        Iterator<Resource> childIterator = childIteratable.iterator();
        while (childIterator.hasNext()) {
            Resource child = childIterator.next();
            visitNode(child);
        }
    }

    /**
     * Parse node information
     *
     * @param resource
     * @throws RepositoryException
     */
    private void parseResource(Resource resource) throws RepositoryException {
        if (isPage(resource)) {
            parsePage(resource);
        } else {
            Node node = resource.adaptTo(Node.class);
            writeNode(node, node.getName());
            writeProperties(node);
        }
    }

    protected void parseCommonNode(Resource resource) throws RepositoryException {
        Node node = resource.adaptTo(Node.class);

    }

    /**
     *
     * @param resource
     * @throws RepositoryException
     */
    protected void parsePage(Resource resource) throws RepositoryException {
        Node node = resource.adaptTo(Node.class);

        // get page
        Page page = context.getRequest().getResourceResolver().getResource(node.getPath()).adaptTo(Page.class);

        writeNode(node, page.getTitle());
        writeProperties(node);

    }

    private void writeNode(Node node, String title) throws RepositoryException {

        context.println(
                "N " +
                    "-name {%s} " +
                    "-path {%s} " +
                    "-type {%s} " +
                    "-sub {%s} " +
                    "-resourceType {%s}",
                escapeCurlyBraces(title),
                getPagePath(node),
                node.getPrimaryNodeType().getName(),
                getSubPath(node),
                escapeCurlyBraces(getResourceType(node))
        );
    }

    protected String getResourceType(Node node) throws RepositoryException {
        if (node.hasProperty("sling:resourceType")) {
            return node.getProperty("sling:resourceType").getString();
        }
        return "";
    }

    /**
     * @return the page path for the current node
     */
    protected String getSubPath(Node node) throws RepositoryException {
        int jcrContentIdx = node.getPath().indexOf("/jcr:content");
        if (jcrContentIdx == -1) {
            return "";
        }
        return node.getPath().substring(jcrContentIdx + 1);
    }


    /**
     * @return a map of all page properties that have been scraped for this page
     */
    protected void writeProperties(Node node) throws RepositoryException {
        String sub = getSubPath(node);
        String pagePath = getPagePath(node);

        PropertyIterator pIt = node.getProperties();
        if (pIt == null) {
            return;
        }

        while (pIt.hasNext()) {
            javax.jcr.Property property = pIt.nextProperty();
            if (property == null) {
                continue;
            }

            Value[] values =
                    property.isMultiple()
                            ? property.getValues()
                            : new Value[] { property.getValue() };

            for (Value value : values) {
                if (value == null) {
                    continue;
                }

                String strValue = valueToString(property, value);
                if (strValue != null) {
                    context.println(
                            "P -path {%s} -sub {%s} -name {%s} -value {%s}",
                            pagePath,
                            sub,
                            property.getName(),
                            escapeCurlyBraces(strValue)
                    );
                }
            }
        }
    }

    protected String escapeCurlyBraces(String strValue) {
        return strValue.replace("{", "\\{").replace("}", "\\}");
    }

    protected String valueToString(javax.jcr.Property prop, Value val) throws RepositoryException {
        switch (prop.getType()) {
            case PropertyType.STRING:
                return val.getString();

            case PropertyType.BOOLEAN:
                return val.getBoolean() ? "true" : "false";

            case PropertyType.LONG:
                return Long.toString(val.getLong());

            case PropertyType.DOUBLE:
                return Double.toString(val.getDouble());

            case PropertyType.DATE:
                return sdFormat.format(val.getDate().getTime());

            case PropertyType.DECIMAL:
                return val.getDecimal().toPlainString();

            default:
                return null;
        }
    }

    /**
     * Write the header
     */
    protected void showHeader() {
        SimpleDateFormat sdFormat = new SimpleDateFormat("HH:mm dd/MM/yyyy");
        context.println("# CQ Content Graph - " + sdFormat.format(new Date()));
        context.println("CLEAR\n");
    }

    protected String getPagePath(Node node) throws RepositoryException {
        String pagePath = node.getPath();
        if (pagePath.indexOf("jcr:content") != -1) {
            pagePath = pagePath.substring(0, pagePath.indexOf("/jcr:content"));
        }
        return pagePath;
    }

    /**
     * @return true if this is a proper page?
     */
    protected boolean isPage(Resource resource) throws RepositoryException {
        Node node = resource.adaptTo(Node.class);
        return node.getPrimaryNodeType().getName().equals("cq:Page");
    }

}
