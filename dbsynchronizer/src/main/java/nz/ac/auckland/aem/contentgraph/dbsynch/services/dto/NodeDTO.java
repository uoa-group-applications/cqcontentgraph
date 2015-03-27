package nz.ac.auckland.aem.contentgraph.dbsynch.services.dto;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.DtoFromRow;

import javax.jcr.Node;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Marnix Cook
 */
public class NodeDTO implements DtoFromRow {

    private Long id;
    private Long parentId;
    private String site;
    private String path;
    private String sub;
    private String resourceType;
    private String type;
    private String title;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getParentId() {
        return parentId;
    }

    public void setParentId(Long parentId) {
        this.parentId = parentId;
    }

    public String getSite() {
        return site;
    }

    public void setSite(String site) {
        this.site = site;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getSub() {
        return sub;
    }

    public void setSub(String sub) {
        this.sub = sub;
    }

    public String getResourceType() {
        return resourceType;
    }

    public void setResourceType(String resourceType) {
        this.resourceType = resourceType;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * @return an instance of NodeDTO based on the contents of the current resultset row content.
     */
    @Override
    public NodeDTO fromRow(ResultSet row) throws SQLException {
        NodeDTO dto = new NodeDTO();

        dto.setId(row.getLong("id"));
        dto.setParentId(row.getLong("parent_id"));
        dto.setSite(row.getString("site"));
        dto.setPath(row.getString("path"));
        dto.setSub(row.getString("sub"));
        dto.setResourceType(row.getString("resourceType"));
        dto.setType(row.getString("type"));
        dto.setTitle(row.getString("title"));

        return dto;
    }

    /**
     * @return the parent path
     */
    public String getParentPath() {
        return path.substring(0, path.lastIndexOf('/'));
    }

}
