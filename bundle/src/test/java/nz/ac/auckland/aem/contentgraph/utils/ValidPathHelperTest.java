package nz.ac.auckland.aem.contentgraph.utils;

import junit.framework.TestCase;
import static org.junit.Assert.*;

public class ValidPathHelperTest extends TestCase {
    
    public void testHelper() throws Exception {
        String rules =
            "+/content/abi\n" +
            "+/etc/tags\n" +
            "-/content/abi/news\n";

        // specified
        assertTrue(ValidPathHelper.isTrackedContentPath("/content/abi/mypath", rules));

        // specified and then excluded
        assertFalse(ValidPathHelper.isTrackedContentPath("/content/abi/news/2015", rules));

        // not specifed
        assertFalse(ValidPathHelper.isTrackedContentPath("/etc/workflow", rules));
    }
}