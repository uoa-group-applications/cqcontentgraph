package nz.ac.auckland.aem.contentgraph.synch;

import com.day.cq.wcm.api.Page;
import nz.ac.auckland.aem.contentgraph.ContentGraphReport;
import nz.ac.auckland.aem.contentgraph.writer.content.ContentWriter;
import nz.ac.auckland.aem.contentgraph.writer.content.ContentWriterFactory;
import nz.ac.auckland.aem.contentgraph.writer.stream.SocketStreamWriter;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.sling.api.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import java.io.IOException;

/**
 * @author Marnix Cook
 *
 * Simple proof-of-concept synchronizer that writes to network socket
 */
@Service
@Component(immediate = true)
public class TclSynchronizer implements Synchronizer {

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(TclSynchronizer.class);

    @Override
    public void delete(String path) {
        ContentWriter writer = ContentWriterFactory.create("tcl");

        SocketStreamWriter stream = null;
        try {
            stream = new SocketStreamWriter("localhost", 10101);
            writer.deleteNode(stream, path);
        }
        catch (RepositoryException rEx) {
            LOG.error("A repository exception occured", rEx);
        }
        catch (IOException ioEx) {
            LOG.error("An exception occured opening the socket", ioEx);
        }
        finally {
            if (stream != null) {
                stream.close();
            }
        }
    }

    /**
     * Synchronization with TCL app
     *
     * @param resource is the node that has changed
     */
    @Override
    public void synch(Resource resource) {

        SocketStreamWriter stream = null;

        try {
            Node node = resource.adaptTo(Node.class);
            ContentWriter writer = ContentWriterFactory.create("tcl");
            stream = new SocketStreamWriter("localhost", 10101);

            if (isPage(node)) {
                Page page = resource.adaptTo(Page.class);
                writer.writeNode(stream, node, page.getTitle());
                writer.writeProperties(stream, node);
            }
            else {
                writer.writeNode(stream, node, node.getName());
                writer.writeProperties(stream, node);
            }

        }
        catch (RepositoryException rEx) {
            LOG.error("A repository exception occurred", rEx);
        }
        catch (IOException ioEx) {
            LOG.error("Could not open socket connection to `localhost:10101`, aborting synch. Library will be out of synch");
        }
        finally {
            if (stream != null) {
                stream.close();
            }
        }
    }

    /**
     * @return true if this is a proper page?
     */
    protected boolean isPage(Node node) throws RepositoryException {
        return node.getPrimaryNodeType().getName().equals(ContentGraphReport.TYPE_PAGE);
    }

    /**
     * @return if the node is an asset
     */
    protected boolean isAsset(Node node) throws RepositoryException {
        return node.getPrimaryNodeType().getName().equals(ContentGraphReport.TYPE_ASSET);
    }


}
