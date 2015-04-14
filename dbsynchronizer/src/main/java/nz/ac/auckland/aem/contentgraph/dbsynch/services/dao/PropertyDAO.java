package nz.ac.auckland.aem.contentgraph.dbsynch.services.dao;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.dto.PropertyDTO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.Database;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper;
import org.apache.commons.lang.NotImplementedException;

import java.sql.*;
import java.util.List;

/**
 * @author Marnix Cook
 *
 * Is the property DAO that contains database operations
 */
public class PropertyDAO implements GenericDAO<PropertyDTO, Long> {

    /**
     * Threshold after which a batch is written
     */
    private static final int BATCH_THRESHOLD = 10000;

    private int currentlyBatched = 0;

    /**
     * Batch insert a list of properties
     *
     * @param db
     * @param properties
     * @return
     * @throws SQLException
     */
    public void insertAll(Database db, List<PropertyDTO> properties) throws SQLException {
        PreparedStatement pStmt = getInsertStatement(db);

        for (PropertyDTO property : properties) {

            int pIdx = 0;
            pStmt.setString(++pIdx, property.getName());
            pStmt.setString(++pIdx, property.getValue());
            pStmt.setLong(++pIdx, property.getNodeId());
            pStmt.setString(++pIdx, property.getSub());
            pStmt.setString(++pIdx, property.getPath());

            pStmt.addBatch();
        }

        executeBatchOnThreshold(pStmt);
    }

    protected void executeBatchOnThreshold(PreparedStatement pStmt) throws SQLException {
        ++currentlyBatched;
        if (currentlyBatched > BATCH_THRESHOLD) {
            pStmt.executeBatch();
            currentlyBatched %= BATCH_THRESHOLD;
        }
    }

    @Override
    public Long insert(Database db, PropertyDTO property) throws SQLException {
        PreparedStatement pStmt = getInsertStatement(db);

        int pIdx = 0;
        pStmt.setString(++pIdx, property.getName());
        pStmt.setString(++pIdx, property.getValue());
        pStmt.setLong(++pIdx, property.getNodeId());
        pStmt.setString(++pIdx, property.getSub());
        pStmt.setString(++pIdx, property.getPath());

        pStmt.executeUpdate();
        return JDBCHelper.getLastInsertedId(pStmt);

    }

    /**
     * Execute the batch on the property insert statement
     *
     * @param db
     * @throws SQLException
     */
    public void executeBatch(Database db) throws SQLException {
        getInsertStatement(db).executeBatch();
    }

    protected PreparedStatement getInsertStatement(Database db) throws SQLException {
        return
            db.preparedStatement(
                "INSERT INTO Property SET name = ?, value = ?, nodeId = ?, sub = ?, path = ?"
            );
    }

    @Override
    public void remove(Database db, Long id) {
        throw new NotImplementedException("`remove` operation is not implemented for PropertyDAO");
    }

    /**
     * Remove all properties that belong to, or are a subpath of <code>path</code>
     *
     * @param db contains the connection to operate on
     * @param path is the path to delete properties for
     *
     * @throws SQLException
     */
    public void removeAll(Database db, String path) throws SQLException {
        PreparedStatement pStmt = db.preparedStatement("DELETE FROM Property WHERE path = ?");
        pStmt.setString(1, path);
        pStmt.execute();
    }



    @Override
    public void update(Database db, Long id, PropertyDTO property) {
        throw new NotImplementedException("`update` operation is not implemented for PropertyDAO");
    }

    @Override
    public void truncate(Database db) throws SQLException {
        JDBCHelper.query(db.getConnection(), "TRUNCATE TABLE Property");
    }
}
