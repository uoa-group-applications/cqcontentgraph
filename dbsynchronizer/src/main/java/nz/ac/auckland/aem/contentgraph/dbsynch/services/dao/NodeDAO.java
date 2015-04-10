package nz.ac.auckland.aem.contentgraph.dbsynch.services.dao;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.dto.NodeDTO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.Database;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * @author Marnix Cook
 *
 * Interfaces with database on Node table
 */
public class NodeDAO implements GenericDAO<NodeDTO, Long> {

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(NodeDAO.class);

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

        Long parentId = getNodeIdForPath(db, dto.getParentPath(), dto.getParentSub());
        Long existingNodeId = getNodeIdForPath(db, dto.getPath(), dto.getSub());

        if (existingNodeId == null) {
            LOG.info("Node not found in DB `{}`", dto.getPath());
            return insertNode(db, dto, parentId);
        } else {
            LOG.info("Replacing node with id `{}`, path `{}`", existingNodeId, dto.getPath());
            return replaceNode(db, dto, existingNodeId);
        }
    }

    /**
     * Replace an existing node information
     *
     * @param db
     * @param dto
     * @param existingNodeId
     * @return
     * @throws SQLException
     */
    protected Long replaceNode(Database db, NodeDTO dto, Long existingNodeId) throws SQLException {
        PreparedStatement stmt =
            db.preparedStatement(
                "UPDATE Node SET " +
                    "path = ?, " +
                    "site = ?, " +
                    "sub = ?, " +
                    "resourceType = ?, " +
                    "type = ?, " +
                    "title = ? " +
                "WHERE " +
                    "id = ?"
            );

        int pIdx = 0;
        stmt.setString(++pIdx, dto.getPath());
        stmt.setString(++pIdx, dto.getSite());
        stmt.setString(++pIdx, dto.getSub());
        stmt.setString(++pIdx, dto.getResourceType());
        stmt.setString(++pIdx, dto.getType());
        stmt.setString(++pIdx, dto.getTitle());
        stmt.setLong(++pIdx, existingNodeId);
        stmt.executeUpdate();

        return existingNodeId;
    }

    /**
     * Insert a new node
     *
     * @param db
     * @param dto
     * @param parentId
     * @return
     * @throws SQLException
     */
    protected Long insertNode(Database db, NodeDTO dto, Long parentId) throws SQLException {
        PreparedStatement stmt =
            db.preparedStatement(
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
            stmt.setNull(++pIdx, Types.INTEGER);
        } else {
            stmt.setLong(++pIdx, parentId);
        }

        stmt.executeUpdate();

        return JDBCHelper.getLastInsertedId(stmt);
    }

    /**
     * @return the id or null for the node with path <code>path</code>
     */
    public Long getNodeIdForPath(Database db, String path, String sub) throws SQLException {

        PreparedStatement pStmt = db.preparedStatement(
                "SELECT id FROM Node WHERE path = ? AND sub = ?"
        );

        pStmt.setString(1, path);
        pStmt.setString(2, sub);
        pStmt.execute();

        // get result
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

        if (path.contains("/jcr:content")) {
            int jcrContentIdx = path.indexOf("/jcr:content");
            String sub = path.substring(jcrContentIdx + 1);
            String strippedPath = path.substring(0, jcrContentIdx);

            PreparedStatement stmt =
                    db.preparedStatement(
                            "DELETE FROM Node WHERE path = ? AND sub LIKE ?"
                    );

            stmt.setString(1, strippedPath);
            stmt.setString(2, sub + "%");
            stmt.execute();
        }
        else {

            PreparedStatement stmt =
                    db.preparedStatement(
                        "DELETE FROM Node WHERE path = ?"
                    );

            stmt.setString(1, path);
            stmt.execute();
        }

    }

    @Override
    public void truncate(Database db) throws SQLException {
        db.preparedStatement("TRUNCATE TABLE Node").execute();
    }


}
