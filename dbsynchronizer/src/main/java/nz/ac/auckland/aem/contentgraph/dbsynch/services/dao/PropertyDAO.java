package nz.ac.auckland.aem.contentgraph.dbsynch.services.dao;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.SQLRunnable;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dto.PropertyDTO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.Database;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper;
import org.apache.commons.lang.NotImplementedException;

import java.sql.*;

import static nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper.escape;
import static nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper.getLastInsertedId;
import static nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper.updateWithCallback;

/**
 * @author Marnix Cook
 *
 * Is the property DAO that contains database operations
 */
public class PropertyDAO implements GenericDAO<PropertyDTO, Long> {

    public static final String PROPERTY_INSERT = "propertyInsert";

    @Override
    public Long insert(Database db, PropertyDTO property) throws SQLException {
        PreparedStatement pStmt = db.preparedStatement(
                PROPERTY_INSERT,
                "INSERT INTO Property SET name = ?, value = ?, nodeId = ?, sub = ?, path = ?"
        );

        int pIdx = 0;
        pStmt.setString(++pIdx, property.getName());
        pStmt.setString(++pIdx, property.getValue());
        pStmt.setLong(++pIdx, property.getNodeId());
        pStmt.setString(++pIdx, property.getSub());
        pStmt.setString(++pIdx, property.getPath());

        pStmt.executeUpdate();
        return JDBCHelper.getLastInsertedId(pStmt);

//
//        String insertQuery =
//                String.format(
//                        "INSERT INTO Property SET name = '%s', value = '%s', nodeId = '%s', sub = '%s', path = '%s'",
//                        escape(property.getName()),
//                        escape(property.getValue()),
//                        escape(property.getNodeId().toString()),
//                        escape(property.getSub()),
//                        escape(property.getPath())
//                );
//
//        Long lastInsertedId =
//            updateWithCallback(db.getConnection(), insertQuery, Long.class, new SQLRunnable<Long>() {
//
//                @Override
//                public Long run(Statement stmt, ResultSet rSet) throws SQLException {
//                    return getLastInsertedId(stmt);
//                }
//            });
//
//        return lastInsertedId;
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
        PreparedStatement pStmt = db.preparedStatement("propertyRemoveAll", "DELETE FROM Property WHERE path = ?");
        pStmt.setString(1, path);
        pStmt.execute();

//        JDBCHelper.query(
//            db.getConnection(),
//            String.format(
//                "DELETE FROM Property WHERE path = '%s'",
//                escape(path)
//            )
//        );
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
