package nz.ac.auckland.aem.contentgraph.dbsynch.services.dao;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.SQLRunnable;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dto.PropertyDTO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.JDBCHelper;
import org.apache.commons.lang.NotImplementedException;

import javax.jcr.Property;
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

    @Override
    public Long insert(Connection conn, PropertyDTO property) throws SQLException {
        String insertQuery =
                String.format(
                    "INSERT INTO Property SET name = '%s', value = '%s', nodeId = '%s', sub = '%s', path = '%s'",
                    escape(property.getName()),
                    escape(property.getValue()),
                    escape(property.getNodeId().toString()),
                    escape(property.getSub()),
                    escape(property.getPath())
                );

        Long lastInsertedId =
            updateWithCallback(conn, insertQuery, Long.class, new SQLRunnable<Long>() {

                @Override
                public Long run(Statement stmt, ResultSet rSet) throws SQLException {
                    return getLastInsertedId(stmt);
                }
            });

        return lastInsertedId;
    }

    @Override
    public void remove(Connection conn, Long id) {
        throw new NotImplementedException("`remove` operation is not implemented for PropertyDAO");
    }

    /**
     * Remove all properties that belong to, or are a subpath of <code>path</code>
     *
     * @param conn is the connection to operate on
     * @param path is the path to delete properties for
     *
     * @throws SQLException
     */
    public void removeAll(Connection conn, String path) throws SQLException {
        JDBCHelper.query(
            conn,
            String.format(
                "DELETE FROM Property WHERE path = '%s'",
                escape(path)
            )
        );
    }



    @Override
    public void update(Connection conn, Long id, PropertyDTO property) {
        throw new NotImplementedException("`update` operation is not implemented for PropertyDAO");
    }

    @Override
    public void truncate(Connection conn) throws SQLException {
        JDBCHelper.query(conn, "TRUNCATE TABLE Property");
    }
}
