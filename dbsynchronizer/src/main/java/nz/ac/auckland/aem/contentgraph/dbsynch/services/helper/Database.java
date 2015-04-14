package nz.ac.auckland.aem.contentgraph.dbsynch.services.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Marnix Cook
 *
 * Wraps a connection and is able to generate named, prepared statements.
 */
public class Database {

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(Database.class);

    /**
     * Connection
     */
    private Connection connection;

    /**
     * Named statements
     */
    private Map<String, PreparedStatement> namedStatements;

    private ConnectionInfo info;

    /**
     * Database wrapper
     *
     * @param conn connection to use
     */
    public Database(Connection conn) {
        this.connection = conn;
        this.namedStatements = new HashMap<String, PreparedStatement>();
    }

    public Database(Connection conn, ConnectionInfo info) {
        this(conn);
        this.info = info;
    }

    public Database(ConnectionInfo info) {
        this(null, info);
    }

    /**
     * Close the database connection
     */
    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
        catch (SQLException sqlEx) {
            LOG.error("An error occured while closing the connection, caused by:", sqlEx);
        }
    }


    /**
     * @return a proper connection instance
     * @throws SQLException
     */
    public Connection getConnection() {
        try {
            if (connection == null || connection.isClosed() || !connection.isValid(1)) {
                boolean oldAutoCommitStatus = connection == null? true : connection.getAutoCommit();
                if (this.info != null) {
                    connection = JDBCHelper.getDatabaseConnection(this.info);
                    connection.setAutoCommit(oldAutoCommitStatus);
                } else {
                    LOG.error("Connection closed, cannot reconnect because no connection information provided");
                }
            }
        }
        catch (SQLException sqlEx) {
            LOG.error("Something went wrong", sqlEx);
        }
        return connection;
    }

    /**
     * Add a new prepared statement
     *
     * @param sql is the SQL statement to prepare
     * @throws SQLException
     */
    public PreparedStatement addPreparedStatement(String sql) throws SQLException {
        if (this.namedStatements.containsKey(sql)) {
            throw new IllegalArgumentException("Cannot add the same prepared statement twice");
        }

        PreparedStatement pSt =
                this
                    .getConnection()
                    .prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

        this.namedStatements.put(sql, pSt);

        return pSt;
    }

    /**
     * Retrieve the prepared statement
     *
     * @param sql is the SQL query to get the associated prepared statement for.
     * @return
     */
    public PreparedStatement getPreparedStatement(String sql) {
        if (!this.namedStatements.containsKey(sql)) {
            throw new IllegalArgumentException("No such prepared statement added: " + sql);
        }

        PreparedStatement pStmt = this.namedStatements.get(sql);
        try {
            if (pStmt.isClosed() || pStmt.getConnection().isClosed()|| !pStmt.getConnection().isValid(1)) {
                LOG.info("Statement was closed, creating a new one");
                this.namedStatements.remove(sql);
                pStmt = addPreparedStatement(sql);
            }
        }
        catch (SQLException sqlEx) {
            LOG.error("SQL Exception occured", sqlEx);
        }

        return pStmt;
    }

    /**
     * @return if the `name` exists in the map.
     */
    public boolean hasPreparedStatement(String name) {
        return this.namedStatements.containsKey(name);
    }


    /**
     * Convenience function for often used pattern in retrieving the prepared statements.
     *
     * @param sql SQL of the prepared statement
     * @return an existing or a new prepared statement object
     * @throws SQLException
     */
    public PreparedStatement preparedStatement(String sql) throws SQLException {
        if (!hasPreparedStatement(sql)) {
            return addPreparedStatement(sql);
        } else {
            return getPreparedStatement(sql);
        }
    }


}
