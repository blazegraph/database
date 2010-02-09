/**
Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package com.bigdata.rdf.sail.remoting;

import java.util.Collection;
import org.openrdf.model.Statement;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;

/**
 * This interface describes a very simple contract between clients and an 
 * instance of a bigdata repository.  Defines basic create, read, delete, and 
 * update operations.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public interface GraphRepository {

    /**
     * Perform a construct query in the syntax of the supplied query language,
     * including or excluding inferred statements per the supplied parameter.
     * 
     * @param query
     *            the construct query to execute
     * @param ql
     *            the query language of the query string
     * @param includeInferred
     *            determines whether evaluation results of this query should
     *            include inferred statements
     * @return 
     *            the query results
     * @throws Exception
     */
    Collection<Statement> executeConstructQuery(String queryString, QueryLanguage ql, 
            boolean includeInferred) throws Exception;

    /**
     * Perform a select query in the syntax of the supplied query language,
     * including or excluding inferred statements per the supplied parameter.
     * 
     * @param query
     *            the construct query to execute
     * @param ql
     *            the query language of the query string
     * @param includeInferred
     *            determines whether evaluation results of this query should
     *            include inferred statements
     * @return 
     *            the query results
     * @throws Exception
     */
    TupleQueryResult executeSelectQuery(String queryString, QueryLanguage ql, 
            boolean includeInferred) throws Exception;

    /**
     * Add the supplied RDF statements to the graph.
     * 
     * @param stmts
     *            rdf statements to add
     * @throws Exception
     */
    void create(Collection<Statement> stmts) throws Exception;

    /**
     * Add the supplied RDF statements to the graph.
     * 
     * @param rdfXml
     *            serialized rdf statements to add
     * @throws Exception
     */
    void create(String rdfXml) throws Exception;

    /**
     * Clears the graph.
     */
    void clear() throws Exception;
    
    /**
     * Delete the supplied RDF statements from the graph.
     * 
     * @param stmts
     *            rdf statements to add
     * @throws Exception
     */
    void delete(Collection<Statement> stmts) throws Exception;

    /**
     * Delete the supplied RDF statements from the graph.
     * 
     * @param rdfXml
     *            serialized rdf statements to delete
     * @throws Exception
     */
    void delete(String rdfXml) throws Exception;

    /**
     * Delete the statements resulting from the specified query from the graph.
     * 
     * @param query
     *            the construct query to execute
     * @param ql
     *            the query language of the query string
     * @throws Exception
     */
    void delete(String queryString, QueryLanguage ql) throws Exception;
    
    /**
     * Add and drop statements in one atomic update.
     *
     * @param rdfXmlToDelete
     *            serialized rdf statements to delete
     * @param rdfXmlToAdd
     *            serialized rdf statements to add
     */
    void update(String rdfXmlToDelete, String rdfXmlToAdd) throws Exception;
    
    /**
     * Add and drop statements in one atomic update.
     *
     * @param toDelete
     *            serialized rdf statements to delete
     * @param toAdd
     *            serialized rdf statements to add
     */
    void update(Collection<Statement> toDelete, Collection<Statement> toAdd) 
            throws Exception;
    
}
