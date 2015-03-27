package nz.ac.auckland.aem.contentgraph.dbsynch.services.helper;

import org.apache.commons.lang3.StringUtils;

/**
 * @author Marnix Cook
 *
 * This class contains JDBC connection information
 */
public class ConnectionInfo {

    /**
     * JDBC connection string
     */
    private String connectionString;

    /**
     * Database username
     */
    private String username;

    /**
     * Database password
     */
    private String password;

    /**
     * Initialize data-members
     *
     * @param connectionString
     * @param username
     * @param password
     */
    public ConnectionInfo(String connectionString, String username, String password) {
        this.connectionString = connectionString;
        this.username = username;
        this.password = password;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    /**
     * @return true if the connection information is incomplete
     */
    public boolean isIncomplete() {
        return
            StringUtils.isBlank(this.connectionString) ||
            StringUtils.isBlank(this.username) ||
            StringUtils.isBlank(this.password);
    }
}
