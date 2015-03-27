package nz.ac.auckland.aem.contentgraph.dbsynch.services.helper;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.SQLRunnable;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * @author Marnix Cook
 *
 * Contains some simple JDBC helper information
 */
public class JDBCHelper {

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(JDBCHelper.class);

    /**
     * Mysql driver classname
     */
    public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";


    /**
     * Helper class has private constructor
     */
    private JDBCHelper() {
    }

    /**
     * Load the JDBC driver
     */
    public static boolean loadJdbcDriver() {
        try {
            Class.forName(MYSQL_DRIVER);
        }
        catch (ClassNotFoundException cnfEx) {
            LOG.error("Could not load the mysql driver, the synchronizer will NOT work", cnfEx);
            return false;
        }

        return true;
    }

    /**
     * Close a connection, take care of catch statement.
     * @param conn
     * @throws SQLException
     */
    public static void closeQuietly(Connection conn) {
        try {
            if (!conn.isClosed()) {
                conn.close();
            }
        }
        catch (SQLException sqlEx) {
            LOG.error("Could not close the connection properly");
        }
    }

    /**
     * Escape a mysql value
     *
     * @param val is the value to escape
     * @return is the escaped version
     */
    public static String escape(String val) {
        return
            val == null
            ? null
            :   val.replace("\\", "\\\\")
                   .replace("\0", "\\0")
                   .replace("\n", "\\n")
                   .replace("\r", "\\r")
                   .replace("'", "\\'")
                   .replace("\"", "\\\"")
            ;
    }

    public static <RT> RT updateWithCallback(Connection conn, String query, Class<RT> rt, SQLRunnable callback) throws SQLException {
        if (conn == null) {
            throw new IllegalArgumentException("`conn` cannot be null");
        }
        if (StringUtils.isBlank(query)) {
            throw new IllegalArgumentException("`query` cannot be null");
        }
        if (callback == null) {
            throw new IllegalArgumentException("`callback` cannot be null");
        }

        Statement stmt = null;

        try {
            stmt = conn.createStatement();
            int affectedRows = stmt.executeUpdate(query, Statement.RETURN_GENERATED_KEYS);
            return (RT) callback.run(stmt, null);
        }
        catch (SQLException sqlEx) {
            LOG.error("An SQL exception occured", sqlEx);
            throw sqlEx;
        }
        finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    public static <RT> RT queryWithCallback(Connection conn, String query, Class<RT> rt, SQLRunnable callback) throws SQLException {
        if (conn == null) {
            throw new IllegalArgumentException("`conn` cannot be null");
        }
        if (StringUtils.isBlank(query)) {
            throw new IllegalArgumentException("`query` cannot be null");
        }
        if (callback == null) {
            throw new IllegalArgumentException("`callback` cannot be null");
        }

        Statement stmt = null;
        ResultSet rSet = null;

        try {
            stmt = conn.createStatement();
            rSet = stmt.executeQuery(query);
            return (RT) callback.run(stmt, rSet);
        }
        finally {
            if (rSet != null) {
                rSet.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    /**
     * Execute a query and return a result set. Make sure to close the resulset when you're done with it.
     *
     * @param conn is the connection to run the query on
     * @param query is the query to run
     * @return is the resultset that is returned
     *
     * @throws SQLException
     */
    public static ResultSet queryWithResultSet(Connection conn, String query) throws SQLException {
        if (conn == null) {
            throw new IllegalArgumentException("`conn` cannot be null");
        }
        if (StringUtils.isBlank(query)) {
            throw new IllegalArgumentException("`query` cannot be null");
        }
        Statement stmt = null;

        try {
            stmt = conn.createStatement();
            stmt.execute(query);
        }
        finally {
            if (stmt != null) {
                stmt.close();
            }
        }
        return null;
    }

    /**
     * Run a simple query that has no result set.
     *
     * @param conn is the connection to run it on
     * @param query is the query to run
     *
     * @throws SQLException
     */
    public static void query(Connection conn, String query) throws SQLException {
        if (conn == null) {
            throw new IllegalArgumentException("`conn` cannot be null");
        }
        if (StringUtils.isBlank(query)) {
            throw new IllegalArgumentException("`query` cannot be null");
        }
        Statement stmt = null;

        try {
            stmt = conn.createStatement();
            stmt.execute(query);
        }
        finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    /**
     * @return database connection instance, or null when incomplete conn info object
     */
    public static Connection getDatabaseConnection(ConnectionInfo connInfo) throws SQLException {
        if (connInfo.isIncomplete()) {
            LOG.error("Cannot connect using an incomplete connection information object");
            return null;
        }

        return DriverManager.getConnection(
                connInfo.getConnectionString(),
                connInfo.getUsername(),
                connInfo.getPassword()
        );
    }

    /**
     * @return the last inserted row id
     */
    public static Long getLastInsertedId(Statement stmt) throws SQLException {
        ResultSet gkSet = stmt.getGeneratedKeys();
        try {
            if (gkSet.next()) {
                return gkSet.getLong(1);
            } else {
                return null;
            }
        }
        finally {
            gkSet.close();
        }
    }


}
