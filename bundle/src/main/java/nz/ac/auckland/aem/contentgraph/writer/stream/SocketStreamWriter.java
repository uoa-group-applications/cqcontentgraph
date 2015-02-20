package nz.ac.auckland.aem.contentgraph.writer.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * @author Marnix Cook
 */
public class SocketStreamWriter implements StreamWriter {

    /**
     * Socket connection
     */
    private Socket socket;

    /**
     * Writer
     */
    private PrintWriter writer;

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(SocketStreamWriter.class);

    /**
     * Initialize socket connection
     *
     * @param host is the host to connect to
     * @param port is the port to connect to
     * @throws IOException when the connection could not be established
     */
    public SocketStreamWriter(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
        this.writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    /**
     * Make sure the socket closes properly
     *
     * @throws IOException
     */
    public void close() {
        try {
            this.writer.close();
            this.socket.close();
        }
        catch (IOException ioEx) {
            LOG.error("Exception during closing socket", ioEx);
        }
    }

    /**
     * Write to socket
     *
     * @param content is the content to write
     */
    @Override
    public void println(String content) {
        this.writer.println(content);
        this.writer.flush();
    }
}
