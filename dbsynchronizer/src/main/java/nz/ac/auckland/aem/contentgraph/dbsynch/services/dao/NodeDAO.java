package nz.ac.auckland.aem.contentgraph.dbsynch.services.dao;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.SQLRunnable;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dto.NodeDTO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.Database;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper;
import org.apache.commons.lang.NotImplementedException;

import java.sql.*;

import static nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper.escape;
import static nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper.updateWithCallback;

/**
 * @author Marnix Cook
 *
 * Interfaces with database on Node table
 */
public class NodeDAO implements GenericDAO<NodeDTO, Long> {

    public static final String NODE_REMOVE_ALL = "nodeRemoveAll";
    public static final String NODE_ID_FOR_PATH = "nodeIdForPath";
    public static final String NODE_TRUNCATE = "nodeTruncate";

    /**
     * Insert a Node DTO into the database and return the id it got.
     *
     * @param db the wrapper object
     * @param dto the dto to insert
     * @return the ID under which it was inserted
     *
     * @throws SQLException when something went wrong inserting the NodeDTO.
     */
    @Override
    public Long insert(Database db, NodeDTO dto) throws SQLException {

        Long parentId = getNodeIdForPath(db, dto.getParentPath());

        PreparedStatement stmt =
            db.preparedStatement(
                "nodeInsertQuery",
                "INSERT INTO Node SET " +
                    "path = ?, " +
                    "site = ?, " +
                    "sub = ?, " +
                    "resourceType = ?, " +
                    "type = ?, " +
                    "title = ?, " +
                    "parent_id = ?"
            );

        int pIdx = 0;
        stmt.setString(++pIdx, dto.getPath());
        stmt.setString(++pIdx, dto.getSite());
        stmt.setString(++pIdx, dto.getSub());
        stmt.setString(++pIdx, dto.getResourceType());
        stmt.setString(++pIdx, dto.getType());
        stmt.setString(++pIdx, dto.getTitle());

        if (parentId == null) {
            stmt.setNull(++pIdx, java.sql.Types.INTEGER);
        } else {
            stmt.setLong(++pIdx, parentId);
        }

        stmt.executeUpdate();

        return JDBCHelper.getLastInsertedId(stmt);
    }

    /**
     * @return the id or null for the node with path <code>path</code>
     */
    public Long getNodeIdForPath(Database db, String path) throws SQLException {

        PreparedStatement pStmt = db.preparedStatement(
            NODE_ID_FOR_PATH,
            "SELECT id FROM Node WHERE path = ?"
        );

        pStmt.setString(1, path);
        pStmt.execute();

        ResultSet rSet = null;
        try {
            rSet = pStmt.getResultSet();
            if (!rSet.next()) {
                return null;
            }
            return rSet.getLong(1);
        }
        finally {
            if (rSet != null && !rSet.isClosed()) {
                rSet.close();
            }
        }
    }


    @Override
    public void remove(Database db, Long id) throws SQLException {
        throw new NotImplementedException("Remove for NodeDAO is not implemented");
    }

    @Override
    public void update(Database db, Long id, NodeDTO dto) throws SQLException {
        throw new NotImplementedException("Update for NodeDAO is not implemented");
    }

    /**
     * Remove all nodes that belong to, or are a subpath of <code>path</code>
     *
     * @param db is the connection to operate on
     * @param path is the path to delete.
     *
     * @throws SQLException
     */
    public void removeAll(Database db, String path) throws SQLException {

        PreparedStatement stmt =
                db.preparedStatement(
                    NODE_REMOVE_ALL,
                    "DELETE FROM Node WHERE path = ?"
                );

        stmt.setString(1, path);
        stmt.execute();
    }

    @Override
    public void truncate(Database db) throws SQLException {
        db.preparedStatement(NODE_TRUNCATE, "TRUNCATE TABLE Node").execute();
    }


}
