package nz.ac.auckland.aem.contentgraph.dbsynch.services.helper;

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
     * Connection
     */
    private Connection connection;

    /**
     * Named statements
     */
    private Map<String, PreparedStatement> namedStatements;

    /**
     * Database wrapper
     *
     * @param conn connection to use
     */
    public Database(Connection conn) {
        this.connection = conn;
        this.namedStatements = new HashMap<String, PreparedStatement>();
    }

    /**
     * Add a new prepared statement
     *
     * @param name is the name of the statement
     * @param sql is the SQL statement to prepare
     * @throws SQLException
     */
    public PreparedStatement addPreparedStatement(String sql) throws SQLException {
        if (this.namedStatements.containsKey(sql)) {
            throw new IllegalArgumentException("Cannot add the same prepared statement twice");
        }

        PreparedStatement pSt = this.connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
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

        return this.namedStatements.get(sql);
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


    /**
     * @return the connection instance
     */
    public Connection getConnection() {
        return this.connection;
    }

}
