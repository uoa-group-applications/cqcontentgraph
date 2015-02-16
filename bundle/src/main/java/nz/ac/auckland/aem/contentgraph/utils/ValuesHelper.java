package nz.ac.auckland.aem.contentgraph.utils;

import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import java.text.SimpleDateFormat;

/**
 * @author Marnix Cook
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

    public static String escapeCurlyBraces(String strValue) {
        if (strValue == null) {
            return null;
        }
        return strValue
                .replace("{", "\\{")
                .replace("}", "\\}")
                .replace("\r", "\\r")
                .replace("\n", "\\n")
             ;
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


    public static String getResourceType(Node node) throws RepositoryException {
        if (node.hasProperty("sling:resourceType")) {
            return node.getProperty("sling:resourceType").getString();
        }
        return "";
    }

    public static String getPagePath(Node node) throws RepositoryException {
        String pagePath = node.getPath();
        if (pagePath.indexOf("jcr:content") != -1) {
            pagePath = pagePath.substring(0, pagePath.indexOf("/jcr:content"));
        }
        return pagePath;
    }

}
