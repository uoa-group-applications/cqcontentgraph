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
    private static final int BATCH_THRESHOLD = 512;

    /**
     * Thread local counter for batched number of properties
     */
    private ThreadLocal<Integer> currentlyBatched = new ThreadLocal<Integer>();

    /**
     * Initialize data-members
     */
    public PropertyDAO() {
        if (currentlyBatched.get() == null) {
            currentlyBatched.set(0);
        }
    }

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

    /**
     * Execute the batch when the threshold has been reached and make sure to
     * reset the threshold to a normal value.
     *
     * @param pStmt
     * @throws SQLException
     */
    protected void executeBatchOnThreshold(PreparedStatement pStmt) throws SQLException {
        int val = currentlyBatched.get();
        ++val;
        if (val > BATCH_THRESHOLD) {
            pStmt.executeBatch();
            pStmt.getConnection().commit();
            val %= BATCH_THRESHOLD;
        }
        currentlyBatched.set(val);
    }

    /**
     * Insert a single property into the database
     *
     * @param db is the database connection to use
     * @param property is the property to write
     * @return the inserted identifier.
     * @throws SQLException
     */
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
                "INSERT DELAYED INTO Property SET name = ?, value = ?, nodeId = ?, sub = ?, path = ?"
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
        db.preparedStatement("TRUNCATE TABLE Property").execute();
    }
}
