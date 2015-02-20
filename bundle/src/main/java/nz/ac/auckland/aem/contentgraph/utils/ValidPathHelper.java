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
     * @param arguments the arguments with the include and exclude rules
     * @return
     */
    public static boolean isTrackedContentPath(String changedPath, String arguments) {
        String[] rules = arguments.split("\n");
        boolean yes = false;
        boolean butNo = false;

        for (String rule : rules) {
            String path = rule.substring(1);
            switch (rule.charAt(0)) {
                case '+':
                    if (changedPath.startsWith(path)) {
                        yes = true;
                    }
                    break;
                case '-':
                    if (changedPath.startsWith(path)) {
                        butNo = true;
                    }
                    break;
                default:
                    LOG.error("Not a valid rule: " + rule + ", skipping!");
            }
        }

        return yes && !butNo;
    }

}
