package nz.ac.auckland.aem.contentgraph.writer;

import nz.ac.auckland.aem.contentgraph.utils.HttpContext;

import javax.jcr.*;
import java.text.SimpleDateFormat;
import java.util.Date;

import static nz.ac.auckland.aem.contentgraph.utils.ValuesHelper.*;

/**
 * @author Marnix Cook
 *
 * This is the Tcl output implementation of the content writer.
 */
public class TclContentWriter implements ContentWriter {

    /**
     * Stores http context
     */
    private HttpContext context;

    /**
     * Initialize data-members
     *
     * @param context is the context to store things in.
     */
    public TclContentWriter(HttpContext context) {
        this.context = context;
    }

    /**
     * Show a header
     */
    @Override
    public void showHeader() {
        SimpleDateFormat sdFormat = new SimpleDateFormat("HH:mm dd/MM/yyyy");
        context.println("# CQ Content Graph - " + sdFormat.format(new Date()));
        context.println("CLEAR\n");
    }

    /**
     * Show a footer
     */
    @Override
    public void showFooter() {
        context.println("\nDONE");
    }

    /**
     * Output a node definition to the browser
     *
     * @param node
     * @param title
     * @throws RepositoryException
     */
    @Override
    public void writeNode(Node node, String title) throws RepositoryException {

        context.println(
                "N " +
                        "-name {%s} " +
                        "-path {%s} " +
                        "-type {%s} " +
                        "-sub {%s} " +
                        "-resourceType {%s} #;",
                escapeCurlyBraces(title),
                getPagePath(node),
                node.getPrimaryNodeType().getName(),
                getSubPath(node),
                escapeCurlyBraces(getResourceType(node))
        );

    }

    /**
     * Write a list of properties
     *
     * @param node
     * @throws RepositoryException
     */
    @Override
    public void writeProperties(Node node) throws RepositoryException {

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
                    writeProperty(sub, pagePath, property, strValue);
                }
            }
        }

    }

    /**
     * Write a single property
     *
     * @param sub
     * @param pagePath
     * @param property
     * @param strValue
     * @throws RepositoryException
     */
    protected void writeProperty(String sub, String pagePath, Property property, String strValue) throws RepositoryException {
        context.println(
                "P -path {%s} -sub {%s} -name {%s} -value {%s} #;",
                pagePath,
                sub,
                property.getName(),
                escapeCurlyBraces(strValue)
        );
    }
}
