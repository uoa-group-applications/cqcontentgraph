package nz.ac.auckland.aem.contentgraph.writer.stream;

import nz.ac.auckland.aem.contentgraph.utils.HttpContext;

/**
 * @author Marnix Cook
 *
 * Simple wrapper for httpcontext writing
 */
public class HttpContextStreamWriter implements StreamWriter {

    private HttpContext context;

    public HttpContextStreamWriter(HttpContext context) {
        this.context = context;
    }

    @Override
    public void println(String content) {
        this.context.println(content);

    }

}
