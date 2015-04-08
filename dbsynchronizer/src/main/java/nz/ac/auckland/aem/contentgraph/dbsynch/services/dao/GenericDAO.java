package nz.ac.auckland.aem.contentgraph.dbsynch.services.dao;

import nz.ac.auckland.aem.contentgraph.dbsynch.services.helper.Database;

import java.sql.*;

/**
 * @author Marnix Cook
 *
 * Simple generic DAO interface for common table operations.
 */
public interface GenericDAO<SourceObject, IdentifierType> {

    /**
     * Insert information from a SourceObject
     *
     * @param info is the information source to insert
     * @return the primary key value
     */
    IdentifierType insert(Database db, SourceObject info) throws SQLException;

    /**
     * Remove a record with identifier <code>id</code>
     *
     * @param id identifier to remove
     */
    void remove(Database db, IdentifierType id) throws SQLException;

    /**
     * Update a record with identifier <code>id</code> with information
     * stored in its source object <code>info</code>.
     *
     * @param id is the identifier to update the row for
     * @param info is the information to store
     */
    void update(Database db, IdentifierType id, SourceObject info) throws SQLException;

    /**
     * Truncate operation on entire table
     */
    void truncate(Database db) throws SQLException;
}
