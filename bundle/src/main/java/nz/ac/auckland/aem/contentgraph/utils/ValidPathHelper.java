package nz.ac.auckland.aem.contentgraph.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Marnix Cook
 */
public class ValidPathHelper {

    private static final Logger LOG = LoggerFactory.getLogger(ValidPathHelper.class);

    private ValidPathHelper() {
    }

    /**
     * This method determines whether the updated information should be synched.
     *
     * @param changedPath is the path that has changes
     * @return
     */
    public static boolean isTrackedContentPath(String changedPath, String[] includes, String[] excludes) {

        boolean yes = false;
        boolean butNo = false;

        for (String path : includes) {
            if (changedPath.startsWith(path)) {
                yes = true;
            }
        }

        for (String path : excludes) {
            if (changedPath.startsWith(path)) {
                butNo = true;
            }
        }

        return yes && !butNo;
    }

}
