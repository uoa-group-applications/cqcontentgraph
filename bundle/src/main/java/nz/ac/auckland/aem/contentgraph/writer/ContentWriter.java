package nz.ac.auckland.aem.contentgraph.writer;

import javax.jcr.Node;
import javax.jcr.RepositoryException;

/**
 * @author Marnix Cook
 */
public interface ContentWriter {

    void showHeader();

    void showFooter();

    void writeNode(Node node, String title) throws RepositoryException;

    /**
     * This method writes
     */
    void writeProperties(Node node) throws RepositoryException;
}
