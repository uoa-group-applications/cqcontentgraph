package nz.ac.auckland.aem.contentgraph.writer.content;

import nz.ac.auckland.aem.contentgraph.utils.HttpContext;
import nz.ac.auckland.aem.contentgraph.writer.stream.StreamWriter;

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
     * Show a header
     */
    @Override
    public void showHeader(StreamWriter writer) {
        SimpleDateFormat sdFormat = new SimpleDateFormat("HH:mm dd/MM/yyyy");
        writer.println("# CQ Content Graph - " + sdFormat.format(new Date()));
        writer.println("CLEAR\n");
    }

    /**
     * Show a footer
     */
    @Override
    public void showFooter(StreamWriter writer) {
        writer.println("\nDONE");
    }

    /**
     * Delete the node at <code>path</code>
     *
     * @param writer is the writer to print the output to
     * @param path is the path that needs to be deleted
     *
     * @throws RepositoryException
     */
    @Override
    public void deleteNode(StreamWriter writer, String path) throws RepositoryException {
        writer.println(
            String.format(
                "D -path {%s}",
                escapeCurlyBraces(path)
            )
        );
    }

    /**
     * Output a node definition to the browser
     *
     * @param node
     * @param title
     * @throws RepositoryException
     */
    @Override
    public void writeNode(StreamWriter writer, Node node, String title) throws RepositoryException {

        writer.println(
                String.format(
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
                )
        );

    }

    /**
     * Write a list of properties
     *
     * @param node
     * @throws RepositoryException
     */
    @Override
    public void writeProperties(StreamWriter writer, Node node) throws RepositoryException {

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
                    writeProperty(writer, sub, pagePath, property, strValue);
                }
            }
        }

    }

    /**
     * Write a single property
     *
     * @param writer
     * @param sub
     * @param pagePath
     * @param property
     * @param strValue
     * @throws RepositoryException
     */
    protected void writeProperty(StreamWriter writer, String sub, String pagePath, Property property, String strValue) throws RepositoryException {
        writer.println(
                String.format(
                        "P -path {%s} -sub {%s} -name {%s} -value {%s} #;",
                        pagePath,
                        sub,
                        property.getName(),
                        escapeCurlyBraces(strValue)
                )
        );
    }
}
