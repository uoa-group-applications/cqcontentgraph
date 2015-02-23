package nz.ac.auckland.aem.contentgraph.utils;

import junit.framework.TestCase;
import static org.junit.Assert.*;

public class ValidPathHelperTest extends TestCase {
    
    public void testHelper() throws Exception {
        String[] include = {"/content/abi", "/etc/tags"};
        String[] exclude = {"/content/abi/news"};

        // specified
        assertTrue(ValidPathHelper.isTrackedContentPath("/content/abi/mypath", include, exclude));

        // specified and then excluded
        assertFalse(ValidPathHelper.isTrackedContentPath("/content/abi/news/2015", include, exclude));

        // not specifed
        assertFalse(ValidPathHelper.isTrackedContentPath("/etc/workflow", include, exclude));
    }
}