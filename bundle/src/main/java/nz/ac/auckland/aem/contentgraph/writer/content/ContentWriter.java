package nz.ac.auckland.aem.contentgraph.writer.content;

import nz.ac.auckland.aem.contentgraph.writer.stream.StreamWriter;

import javax.jcr.Node;
import javax.jcr.RepositoryException;

/**
 * @author Marnix Cook
 *
 * A content writer allows you to implement different formats in which
 * the JCR node information is marshalled to whatever output writer
 * you may have lined up.
 */
public interface ContentWriter {

    /**
     * Will write header information for this content writer
     * to the output stream writer
     *
     * @param writer is the stream to write to
     */
    void showHeader(StreamWriter writer);

    /**
     * Will output footer information to the stream writer
     *
     * @param writer is the stream to write to
     */
    void showFooter(StreamWriter writer);

    /**
     * Indicates the deletion of a node
     *
     * @param writer
     * @param path
     * @throws RepositoryException
     */
    void deleteNode(StreamWriter writer, String path) throws RepositoryException;

    /**
     * Write a complete node to the stream writer
     *
     * @param writer is the stream writer to print to
     * @param node is the node to get the properties to
     * @param title is the title of the node
     * @throws RepositoryException
     */
    void writeNode(StreamWriter writer, Node node, String title) throws RepositoryException;

    /**
     * This method writes all properties of node <code>node</code> to the streamwriter
     *
     * @param writer is the stream writer to print to
     * @param node is the node to get the properties from
     */
    void writeProperties(StreamWriter writer, Node node) throws RepositoryException;
}
