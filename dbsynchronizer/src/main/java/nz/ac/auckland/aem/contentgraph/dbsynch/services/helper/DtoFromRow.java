package nz.ac.auckland.aem.contentgraph.dbsynch.services.helper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Marnix Cook
 */
public interface DtoFromRow {

    public <DTO> DTO fromRow(ResultSet row) throws SQLException;

}
