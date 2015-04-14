package nz.ac.auckland.aem.contentgraph.dbsynch.services.dao;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.dto.NodeDTO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.Database;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper;
import nz.ac.auckland.aem.contentgraph.utils.PerformanceReport;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

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

    private static Map<String, Long> nodePathIdMap = new HashMap<String, Long>();

    /**
     * Reset the mapping
     */
    public static void resetMapping() {
        nodePathIdMap = new HashMap<String, Long>();
    }


    /**
     * The force insert is used in the case of a full reindex. It assumes that as
     * nodes are being inserted, none with the same path exist. This is to prevent
     * us from having to do costly SELECT statements that have a known outcome.
     *
     * Note: Should not have the preference in normal use cases.
     *
     * @param db the database to write to
     * @param dto the node information to write
     * @return the newly inserted identifier
     *
     * @throws SQLException when something goes wrong
     */
    public Long forceInsert(Database db, NodeDTO dto) throws SQLException {

        Long parentId = getNodeIdForPath(db, dto.getParentPath(), dto.getParentSub());
        Long newId = insertNode(db, dto, parentId);
        setNodeIdForPath(dto.getPath(), dto.getSub(), newId);

        return newId;
    }


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

        Long start = System.currentTimeMillis();
        Long parentId = getNodeIdForPath(db, dto.getParentPath(), dto.getParentSub());
        Long existingNodeId = getNodeIdForPath(db, dto.getPath(), dto.getSub());
        PerformanceReport.getInstance().addToCategory("pre-insert selects", System.currentTimeMillis() - start);

        if (existingNodeId == null) {
            LOG.info("Node not found in DB `{}`", dto.getPath());

            Long newId = insertNode(db, dto, parentId);
            setNodeIdForPath(dto.getPath(), dto.getSub(), newId);
            return newId;

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


    protected void setNodeIdForPath(String path, String sub, Long newId) {
        nodePathIdMap.put(path + "/" + sub, newId);
    }

    public Long getNodeIdForPath(Database db, String path, String sub) throws SQLException {
        String mapKey = path + "/" + sub;
        if (nodePathIdMap.containsKey(mapKey)) {
            return nodePathIdMap.get(mapKey);
        } else {
            Long id = getNodeIdForPathDb(db, path, sub);
            setNodeIdForPath(path, sub, id);
            return id;
        }
    }

    /**
     * @return the id or null for the node with path <code>path</code>
     */
    public Long getNodeIdForPathDb(Database db, String path, String sub) throws SQLException {

        PreparedStatement pStmt =
            db.preparedStatement(
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
