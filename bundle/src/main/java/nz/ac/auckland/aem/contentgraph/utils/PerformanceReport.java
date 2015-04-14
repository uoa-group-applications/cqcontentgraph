package nz.ac.auckland.aem.contentgraph.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marnix Cook
 */
public class PerformanceReport {

    private static final PerformanceReport INSTANCE = new PerformanceReport();
    private Map<String, Long> categorySpentMap;

    private PerformanceReport() {
        // private constructor, singleton
        resetMap();
    }


    public void resetMap() {
        this.categorySpentMap = new HashMap<String, Long>();
    }

    public Map<String, Long> getMap() {
        return this.categorySpentMap;
    }

    /**
     * Add amount of time to category
     *
     * @param catName
     * @param spent
     */
    public void addToCategory(String catName, Long spent) {

        // new category? put in map
        if (!categorySpentMap.containsKey(catName)) {
            categorySpentMap.put(catName, spent);
        } else {

            // otherwise, add to existing number
            categorySpentMap.put(
                catName,
                categorySpentMap.get(catName) + spent
            );
        }
    }


    public static PerformanceReport getInstance() {
        return INSTANCE;
    }


}
