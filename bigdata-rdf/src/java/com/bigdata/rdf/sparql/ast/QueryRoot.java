/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
package com.bigdata.rdf.sparql.ast;

import java.util.Enumeration;
import java.util.Properties;

import org.openrdf.query.parser.sparql.ast.SimpleNode;

import com.bigdata.rdf.sail.QueryHints;
import com.bigdata.rdf.sail.QueryType;

/**
 * The top-level Query.
 * 
 * @see DatasetNode
 * @see IGroupNode
 * @see ProjectionNode
 * @see GroupByNode
 * @see HavingNode
 * @see OrderByNode
 * @see SliceNode
 */
public class QueryRoot extends QueryBase {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends QueryBase.Annotations {

        /**
         * The original query from which this AST was generated.
         */
        String QUERY_STRING = "queryString";

        /**
         * The parse tree generated from the query string (optional). For the
         * default integration, this is the parse tree assembled by the Sesame
         * <code>sparql.jjt</code> grammar. Other integrations may produce
         * different parse trees using different object models.
         * <p>
         * Note: There is no guarantee that the parse tree is a serializable
         * object. It may not need to be stripped off of the {@link QueryRoot}
         * if the {@link QueryRoot} is persisted or shipped to another node in a
         * cluster.
         */
        String PARSE_TREE = "parseTree";

        /**
         * Query hints (optional). When present, this is a {@link Properties}
         * object.
         * 
         * @see QueryHints
         */
        String QUERY_HINTS = "queryHints";

        /**
         * The {@link DatasetNode}.
         */
        String DATASET = "dataset";
        
        /**
         * The {@link NamedSubqueriesNode} (optional).
         */
        String NAMED_SUBQUERIES = "namedSubqueries";
        
    }

    public QueryRoot(final QueryType queryType) {
        
        super(queryType);
        
    }

    /**
     * This is a root node. It may not be attached as a child of another node.
     * 
     * @throws UnsupportedOperationException
     */
    public void setParent(final IGroupNode<?> parent) {
    
        throw new UnsupportedOperationException();
        
    }
    
    /**
     * Return the original query from which this AST model was generated.
     */
    public String getQueryString() {

        return (String) getProperty(Annotations.QUERY_STRING);

    }

    /**
     * Set the query string used to generate the AST model.
     * @param queryString The query string.
     */
    public void setQueryString(String queryString) {
        
        setProperty(Annotations.QUERY_STRING, queryString);
        
    }

    /**
     * Return the parse tree generated from the query string. 
     */
    public Object getParseTree() {

        return getProperty(Annotations.PARSE_TREE);
        
    }

    /**
     * Set the parse tree generated from the query string.
     * 
     * @param parseTree
     *            The parse tree (may be <code>null</code>).
     */
    public void setParseTree(Object parseTree) {
        
        setProperty(Annotations.PARSE_TREE, parseTree);
        
    }
    
    /**
     * Return the optional query hints.
     * 
     * @see QueryHints
     */
    public Properties getQueryHints() {
        
        return (Properties) getProperty(Annotations.QUERY_HINTS);
        
    }
    
    /**
     * Set the query hints.
     * 
     * @param queryHints
     *            The query hints (may be <code>null</code>).
     *            
     * @see QueryHints
     */
    public void setQueryHints(final Properties queryHints) {

        setProperty(Annotations.QUERY_HINTS, queryHints);
        
    }

    /**
     * Set the dataset.
     * 
     * @param dataset
     */
    public void setDataset(final DatasetNode dataset) {

        setProperty(Annotations.DATASET, dataset);

    }

    /**
     * Return the dataset.
     */
    public DatasetNode getDataset() {

        return (DatasetNode) getProperty(Annotations.DATASET);

    }
    
    /**
     * Return the node for the named subqueries -or- <code>null</code> if there
     * it does not exist.
     */
    public NamedSubqueriesNode getNamedSubqueries() {
        
        return (NamedSubqueriesNode) getProperty(Annotations.NAMED_SUBQUERIES);
        
    }
    
    /**
     * Set or clear the named subqueries node.
     * 
     * @param namedSubqueries
     *            The named subqueries not (may be <code>null</code>).
     */
    public void setNamedSubqueries(final NamedSubqueriesNode namedSubqueries) {

        setProperty(Annotations.NAMED_SUBQUERIES, namedSubqueries);

    }

    public String toString(final int indent) {
        
        final String s = indent(indent);
        
        final StringBuilder sb = new StringBuilder();

        final String queryString = getQueryString();
        
        final Object parseTree = getParseTree();
        
        final Properties queryHints = getQueryHints();
        
        final DatasetNode dataset = getDataset();

        final NamedSubqueriesNode namedSubqueries = getNamedSubqueries();

        if (queryString != null) {

            sb.append(s);
            sb.append(queryString);
            sb.append("\n");

        }
        
        if (parseTree != null) {

            if(parseTree instanceof SimpleNode) {

                // Dump parse tree for sparql.jjt grammar.
                sb.append(((SimpleNode)parseTree).dump(s));
                
            } else {
            
                /*
                 * Dump some other parse tree, assuming it implements toString()
                 * as pretty print.
                 */
                sb.append(s);
                sb.append(parseTree.toString());
                sb.append("\n");
                
            }

        }
        
        if (queryHints != null) {

            @SuppressWarnings({ "unchecked", "rawtypes" })
            final Enumeration<String> eitr = (Enumeration) queryHints
                    .propertyNames();
            
            while(eitr.hasMoreElements()) {
                
                final String key = eitr.nextElement();

                sb.append("\n");
                sb.append(s);
                sb.append("hint: [" + key + "]=[" + queryHints.getProperty(key)
                        + "]");
                
            }
            
        }
        
        if (dataset != null) {

            sb.append("\n");
            
            sb.append(s);
            
            sb.append(dataset.toString());
            
        }

        if (namedSubqueries != null && !namedSubqueries.isEmpty()) {

            sb.append("\n");
            
            sb.append(s);
            
            sb.append("named subqueries");
            
            sb.append(namedSubqueries.toString(indent));

        }
        
        sb.append(super.toString(indent));
        
        return sb.toString();
        
    }

}
