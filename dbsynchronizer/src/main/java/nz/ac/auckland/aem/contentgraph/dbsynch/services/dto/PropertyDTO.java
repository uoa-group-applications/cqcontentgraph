package nz.ac.auckland.aem.contentgraph.dbsynch.services.dto;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.DtoFromRow;
import org.apache.commons.lang.NotImplementedException;

import javax.jcr.Node;
import javax.jcr.Property;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * @author Marnix Cook
 */
public class PropertyDTO implements DtoFromRow {

    private Long nodeId;
    private String sub;
    private String name;
    private String value;
    private String path;

    public Long getNodeId() {
        return nodeId;
    }

    public void setNodeId(Long nodeId) {
        this.nodeId = nodeId;
    }

    public String getSub() {
        return sub;
    }

    public void setSub(String sub) {
        this.sub = sub;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    /**
     * @return an instance of this class with its fields populated by the current result row
     */
    @Override
    public PropertyDTO fromRow(ResultSet row) throws SQLException {
        PropertyDTO dto = new PropertyDTO();
        dto.setName(row.getString("name"));
        dto.setNodeId(row.getLong("nodeId"));
        dto.setPath(row.getString("path"));
        dto.setSub(row.getString("sub"));
        dto.setValue(row.getString("value"));
        return dto;
    }


}
