package nz.ac.auckland.aem.contentgraph.dbsynch.services;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Marnix Cook
 *
 * This is a simple runnable implementation that needs a resultset to be passed to it.
 */
public interface SQLRunnable<ReturnType> {

    ReturnType run(Statement stmt, ResultSet rSet) throws SQLException;

}
