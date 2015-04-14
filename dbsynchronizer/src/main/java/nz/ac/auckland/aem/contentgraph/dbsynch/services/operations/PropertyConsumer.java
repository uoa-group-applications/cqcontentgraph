package nz.ac.auckland.aem.contentgraph.dbsynch.services.operations;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.dao.PropertyDAO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.dto.PropertyDTO;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.Database;
import nz.ac.auckland.aem.contentgraph.dbsynch.services.visitors.ReindexPersistSynchVisitor;
import nz.ac.auckland.aem.contentgraph.utils.PerformanceReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * @author Marnix Cook
 *
 * Consumer of Property DTO instances
 */
public class PropertyConsumer implements Runnable {

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(PropertyConsumer.class);

    /**
     * Queue to read from
     */
    private BlockingQueue<List<PropertyDTO>> propQueue;

    /**
     * Database
     */
    private Database db;

    private boolean done = false;

    /**
     * Property consumer
     *
     * @param db database connection to use
     * @param propQueue the property queue to take from
     */
    public PropertyConsumer(Database db, BlockingQueue<List<PropertyDTO>> propQueue) {
        this.db = db;
        this.propQueue = propQueue;
    }

    /**
     * Finish processing
     */
    public void commit() {
        try {
            new PropertyDAO().executeBatch(db);
            db.getConnection().commit();
        }
        catch (SQLException sqlEx) {
            LOG.error("Error committing transaction", sqlEx);
        }
    }

    /**
     * Take from the queue
     */
    public void run() {
        PropertyDAO propertyDao = getPropertyDAO();

        while (!done) {
            try {
                List<PropertyDTO> dtos = propQueue.take();

                Long startTime = System.currentTimeMillis();

                // persist
                propertyDao.insertAll(db, dtos);

                // record length of writing
                PerformanceReport.getInstance().addToCategory(
                        Thread.currentThread().getName(),
                        System.currentTimeMillis() - startTime
                );

                // log information
                LOG.debug(
                    String.format(
                        "[%s]: storing %d properties",
                        Thread.currentThread().getName(),
                        dtos.size()
                    )
                );
            }
            catch (SQLException sqlEx) {
                LOG.error("Error writing properties, caused by", sqlEx);
            }
            catch (InterruptedException iEx) {
                LOG.error("Interrupted consumer, caused by:", iEx);
            }
        }

    }

    protected PropertyDAO getPropertyDAO() {
        return new PropertyDAO();
    }
}
