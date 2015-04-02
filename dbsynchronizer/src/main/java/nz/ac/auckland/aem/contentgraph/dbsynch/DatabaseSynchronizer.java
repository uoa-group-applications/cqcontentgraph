package nz.ac.auckland.aem.contentgraph.dbsynch;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.ConnectionInfo;
import nz.ac.auckland.aem.contentgraph.synch.Synchronizer;

/**
 * @author Marnix Cook
 *
 * Interface description for the database synchronization implementation
 */
public interface DatabaseSynchronizer extends Synchronizer {

    public static final String BUNDLE_PARAM_ENABLED = "enabled";
    public static final String BUNDLE_PARAM_JDBC = "jdbc";
    public static final String BUNDLE_PARAM_USER = "user";
    public static final String BUNDLE_PARAM_PASSWORD = "password";

    public ConnectionInfo getConnectionInfo();

}
