package nz.ac.auckland.aem.contentgraph;

import nz.ac.auckland.aem.contentgraph.writer.content.ContentWriter;

/**
 * @author Marnix Cook
 */
public class GraphReportConfig {

    private String[] paths;
    private String[] exclude;
    private ContentWriter writer;

    /**
     * Initialize data-members
     *
     * @param paths
     * @param exclude
     * @param writer
     */
    public GraphReportConfig(String[] paths, String[] exclude, ContentWriter writer) {
        this.paths = paths;
        this.exclude = exclude;
        this.writer = writer;
    }

    public String[] getPaths() {
        return paths;
    }

    public String[] getExclude() {
        return exclude;
    }

    public ContentWriter getWriter() {
        return writer;
    }

    /**
     * @return true if the path is in the exclusions
     */
    public boolean isExcludedPath(String path) {
        for (String exclusion : exclude) {
            if (path.startsWith(exclusion)) {
                return true;
            }
        }
        return false;
    }

}
