package nz.ac.auckland.aem.contentgraph.dbsynch.services.operations;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.dto.NodeDTO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dto.PropertyDTO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.ValuesHelper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.*;
import java.util.ArrayList;
import java.util.List;

import static nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.ValuesHelper.*;

/**
 * @author Marnix Cook
 *
 * This class contains methods that allow us to transform JCR instances into
 * something we can communicate to the database with.
 */
public class NodeTransform {

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(NodeTransform.class);

    /**
     * Ingest values from a JCR node and return a node dto
     *
     * @param jcrNode is the JCR node to transform into a DTO
     * @return the DTO instance, or null when something went wrong.
     */
    public NodeDTO getNodeDTO(Node jcrNode) {
        try {
            NodeDTO dto = new NodeDTO();
            dto.setPath(getPagePath(jcrNode));
            dto.setResourceType(getResourceType(jcrNode));
            dto.setSub(getSubPath(jcrNode));
            dto.setType(jcrNode.getPrimaryNodeType().getName());
            dto.setSite(getSitePath(jcrNode));

            // TODO: set title to page title if node is cq:Page type
            dto.setTitle(jcrNode.getName());

            return dto;
        }
        catch (RepositoryException rEx) {
            LOG.error("Cannot create node DTO, repository exception occurred", rEx);
        }
        return null;
    }

    /**
     * @return a list of property DTO objects based on the node that is passed to this method.
     */
    public List<PropertyDTO> getPropertyDTOList(Node jcrNode) {
        if (jcrNode == null) {
            throw new IllegalArgumentException("`jcrNode` cannot be null");
        }
        try {

            // will contain all properties
            List<PropertyDTO> completeDtoList = new ArrayList<PropertyDTO>();
            PropertyIterator pIterator = jcrNode.getProperties();

            while (pIterator.hasNext()) {
                Property prop = pIterator.nextProperty();

                // each property can have multiple values, each value has its own record
                List<PropertyDTO> dtoList = getPropertyDTO(jcrNode, prop);

                if (CollectionUtils.isNotEmpty(dtoList)) {
                    completeDtoList.addAll(dtoList);
                }
            }

            return completeDtoList;
        }
        catch (RepositoryException rEx) {
            LOG.error("Repository exception caught", rEx);
        }
        return null;
    }

    /**
     * @return the dto instance for the jcr property
     */
    protected List<PropertyDTO> getPropertyDTO(Node parentNode, Property prop) throws RepositoryException {
        if (prop == null) {
            throw new IllegalArgumentException("`prop` cannot be null");
        }

        List<PropertyDTO> dtoList = new ArrayList<PropertyDTO>();

        // always have a list of values
        Value[] values =
                prop.isMultiple()
                        ? prop.getValues()
                        : new Value[] { prop.getValue() };

        // iterate over all values
        for (Value val : values) {

            PropertyDTO dto = new PropertyDTO();

            dto.setName(prop.getName());
            dto.setValue(ValuesHelper.valueToString(prop, val));
            dto.setPath(getPagePath(parentNode));
            dto.setSub(getSubPath(parentNode));

            dtoList.add(dto);
        }

        return dtoList;
    }

    /**
     * @return true if this is a proper page?
     */
    protected boolean isPage(Node node) throws RepositoryException {
        return node.getPrimaryNodeType().getName().equals("cq:Page");
    }
}
