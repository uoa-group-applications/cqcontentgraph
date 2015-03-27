package nz.ac.auckland.aem.contentgraph.dbsynch.services.dao;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.SQLRunnable;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dto.NodeDTO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper;
import org.apache.commons.lang.NotImplementedException;

import java.sql.*;

import static nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper.escape;
import static nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper.queryWithCallback;
import static nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper.updateWithCallback;

/**
 * @author Marnix Cook
 *
 * Interfaces with database on Node table
 */
public class NodeDAO implements GenericDAO<NodeDTO, Long> {

    private PropertyDAO propertyDao = new PropertyDAO();

    /**
     * Insert a Node DTO into the database and return the id it got.
     *
     * @param conn the connection object
     * @param dto the dto to insert
     * @return the ID under which it was inserted
     *
     * @throws SQLException when something went wrong inserting the NodeDTO.
     */
    @Override
    public Long insert(Connection conn, NodeDTO dto) throws SQLException {

        Long parentId = getNodeIdForPath(conn, dto.getParentPath());

        String insertQuery =
                String.format(
                    "INSERT INTO Node SET " +
                            "path = '%s', " +
                            "site = '%s', " +
                            "sub = '%s', " +
                            "resourceType = '%s', " +
                            "type = '%s', " +
                            "title = '%s', " +
                            "parent_id = %s",

                    escape(dto.getPath()),
                    escape(dto.getSite()),
                    escape(dto.getSub()),
                    escape(dto.getResourceType()),
                    escape(dto.getType()),
                    escape(dto.getTitle()),
                    parentId == null? "NULL" : parentId.toString()
                );

        Long lastInsertedId =
            updateWithCallback(conn, insertQuery, Long.class, new SQLRunnable<Long>() {

        @Override
        public Long run(Statement stmt, ResultSet rSet) throws SQLException {
                return JDBCHelper.getLastInsertedId(stmt);
                    }
            });

        return lastInsertedId;
    }

    /**
     * @return the id or null for the node with path <code>path</code>
     */
    public Long getNodeIdForPath(Connection conn, String path) throws SQLException {
        String query = String.format("SELECT id FROM Node WHERE path = '%s'", escape(path));
        return JDBCHelper.queryWithCallback(conn, query, Long.class, new SQLRunnable<Long>() {
            public Long run(Statement stmt, ResultSet rSet) throws SQLException {
                if (!rSet.next()) {
                    return null;
                }
                return rSet.getLong(1);
            }
        });
    }


    @Override
    public void remove(Connection conn, Long id) throws SQLException {
        throw new NotImplementedException("Remove for NodeDAO is not implemented");
    }

    @Override
    public void update(Connection conn, Long id, NodeDTO dto) throws SQLException {
        throw new NotImplementedException("Update for NodeDAO is not implemented");
    }

    /**
     * Remove all nodes that belong to, or are a subpath of <code>path</code>
     *
     * @param con is the connection to operate on
     * @param path is the path to delete.
     *
     * @throws SQLException
     */
    public void removeAll(Connection con, String path) throws SQLException {
        JDBCHelper.query(
            con,
            String.format(
                "DELETE FROM Node WHERE path = '%s'",
                escape(path)
            )
        );
    }


    @Override
    public void truncate(Connection conn) throws SQLException {
        JDBCHelper.query(conn, "TRUNCATE TABLE Node");
    }
}
