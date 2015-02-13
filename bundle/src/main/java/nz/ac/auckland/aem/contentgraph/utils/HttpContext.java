package nz.ac.auckland.aem.contentgraph.utils;

import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;

import java.io.IOException;

/**
 * This service allows for the easy storage, manipulation and access of the current threads
 * {@link org.apache.sling.api.SlingHttpServletRequest request} and {@link org.apache.sling.api.SlingHttpServletResponse response}.
 * <p>Author: <a href="http://gplus.to/tzrlk">Peter Cummuskey</a></p>
 */
public interface HttpContext extends AutoCloseable {

    /**
     * Used to set the current context for later retrieval. Should be done as the first operation in a servlet.
     * @param request The current servlet request.
     * @param response The current servlet response.
     */
    public void setContext(SlingHttpServletRequest request, SlingHttpServletResponse response);

    /**
     * Simply provides access to the previously stored request.
     * @return The previously stored request.
     */
    public SlingHttpServletRequest getRequest();

    /**
     * Simply provides access to the previously stored response.
     * @return The previously stored response.
     */
    public SlingHttpServletResponse getResponse();

    /**
     * Acts as a shortcut to {@link javax.servlet.http.HttpServletRequest#getHeader}.
     * @param name The name of the header to retrieve.
     * @return The value stored under that header.
     */
    public String getHeader(String name);

    /**
     * Acts as a shortcut to {@link javax.servlet.http.HttpServletResponse#setHeader}
     * @param name The name of the header to set.
     * @param value The value to set for that header.
     */
    public void setHeader(String name, String value);

    /**
     * Prints a basic string message to the response output.
     * @param message The message to print.
     * @param args arguments to replace in the message.
     */
    public void print(String message, Object... args);

    /**
     * Prints a basic string message line to the response output.
     * @param message The message to print.
     * @param args arguments to replace in the message.
     */
    void println(String message, Object... args);

    /**
     * Prints a throwable error stacktrace to the response output.
     * @param throwable The throwable error to print.
     */
    void println(Throwable throwable);

    /**
     * Final tasks to perform on the context before ending the request.
     * @throws java.io.IOException
     */
    void close()
            throws IOException;
}
