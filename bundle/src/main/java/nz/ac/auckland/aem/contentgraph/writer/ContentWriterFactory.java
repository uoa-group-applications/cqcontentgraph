package nz.ac.auckland.aem.contentgraph.writer;

import nz.ac.auckland.aem.contentgraph.utils.HttpContext;
import org.apache.commons.lang.NotImplementedException;

/**
 * @author Marnix Cook
 */
public class ContentWriterFactory {

    /**
     * @return the appropriate content writer instance
     */
    public static ContentWriter create(HttpContext context, String type) {
        if ("tcl".equals(type)) {
            return new TclContentWriter(context);
        }
        if ("json".equals(type)) {
            throw new NotImplementedException("Yet to implement the json content writer");
        }

        throw new IllegalArgumentException("Expected `tcl` or `json`");
    }
}
