package nz.ac.auckland.aem.contentgraph.dbsynch.services.helper;

import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import java.text.SimpleDateFormat;

/**
 * @author Marnix Cook
 *
 * Contains some helper functions that parses values from nodes and properties.
 */
public class ValuesHelper {

    private static SimpleDateFormat sdFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm");

    private ValuesHelper() {
    }

    public static String valueToString(javax.jcr.Property prop, Value val) throws RepositoryException {
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
     * @return the page path for the current node
     */
    public static String getSubPath(Node node) throws RepositoryException {
        int jcrContentIdx = node.getPath().indexOf("/jcr:content");
        if (jcrContentIdx == -1) {
            return "";
        }
        return node.getPath().substring(jcrContentIdx + 1);
    }


    /**
     * @return the resource type for this node
     */
    public static String getResourceType(Node node) throws RepositoryException {
        if (node.hasProperty("sling:resourceType")) {
            return node.getProperty("sling:resourceType").getString();
        }
        return "";
    }

    /**
     * @return the page path for this node
     */
    public static String getPagePath(Node node) throws RepositoryException {
        String pagePath = node.getPath();
        if (pagePath.indexOf("jcr:content") != -1) {
            pagePath = pagePath.substring(0, pagePath.indexOf("/jcr:content"));
        }
        return pagePath;
    }

    /**
     * @return the site path of the node
     */
    public static String getSitePath(Node node) throws RepositoryException {
        String nodePath = node.getPath();
        int slashAfterContentPath = nodePath.indexOf('/', "/content/".length());
        if (slashAfterContentPath == -1) {
            return nodePath;
        }
        return nodePath.substring(0, slashAfterContentPath);
    }

}
