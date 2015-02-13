package nz.ac.auckland.aem.contentgraph.utils;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.osgi.framework.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * {@inheritDoc}
 * This is the default implementation of the {@link HttpContext} service definition.
 * <p>Author: <a href="http://gplus.to/tzrlk">Peter Cummuskey</a></p>
 */
@Service
@Component
public class HttpContextService implements HttpContext {
    private static final Logger logger = LoggerFactory.getLogger(HttpContextService.class);

    @Property("A service to make managing http requests and responses easier")
    public static final String DESCRIPTION = Constants.SERVICE_DESCRIPTION;

    @Property("University of Auckland")
    public static final String VENDOR = Constants.SERVICE_VENDOR;

    private static final ThreadLocal<SlingHttpServletRequest> handleRequest = new ThreadLocal<SlingHttpServletRequest>();
    private static final ThreadLocal<SlingHttpServletResponse> handleResponse = new ThreadLocal<SlingHttpServletResponse>();

    /** {@inheritDoc} */
    @Override
    public void setContext(SlingHttpServletRequest request, SlingHttpServletResponse response) {
        handleRequest.set(request);
        handleResponse.set(response);
    }

    /** {@inheritDoc} */
    @Override
    public SlingHttpServletRequest getRequest() {
        return handleRequest.get();
    }

    /** {@inheritDoc} */
    @Override
    public SlingHttpServletResponse getResponse() {
        return handleResponse.get();
    }

    /** {@inheritDoc} */
    @Override
    public String getHeader(String name) {
        return getRequest().getHeader(name);
    }

    /** {@inheritDoc} */
    @Override
    public void setHeader(String name, String value) {
        getResponse().setHeader(name, value);
    }

    /** {@inheritDoc} */
    @Override
    public void print(String message, Object... args) {
        internalPrint(message, args, false);
    }

    /** {@inheritDoc} */
    @Override
    public void println(String message, Object... args) {
        internalPrint(message, args, true);
    }

    protected void internalPrint(String message, Object[] args, boolean newline) {
        String formattedMessage;
        if ( args != null && args.length != 0 ) {
            formattedMessage = String.format(message, args);
        } else {
            formattedMessage = message;
        }

        try {
            PrintWriter writer = this.getResponse().getWriter();
            if (newline) {
                writer.println(formattedMessage);
            } else {
                writer.print(formattedMessage);
            }
        } catch (IOException error) {
            logger.warn(
                    "An {} occurred attempting to get the response writer: {}",
                    error.getClass().getSimpleName(),
                    error.getMessage()
            );
        }
    }

    /** {@inheritDoc} */
    @Override
    public void println(Throwable throwable) {
        String stackTrace = convertThrowable(throwable);
        this.println(stackTrace);
    }

    protected String convertThrowable(Throwable throwable) {
        // Set up the IO utils to fake out the stream.
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);

        // Print the stacktrace to the writer
        throwable.printStackTrace(printWriter);

        // Run the resulting string through println(String)
        return stringWriter.toString();
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
        getResponse().getWriter().flush();
    }

}
