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

import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

import com.bigdata.bop.BOp;

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
         * The namespace prefix declaration map. This is a {@link Map} with
         * {@link String} keys (prefix) and {@link String} values (the uri
         * associated with that prefix).
         */
        String PREFIX_DECLS = "prefixDecls";
        
        /**
         * The {@link DatasetNode}.
         */
        String DATASET = "dataset";
        
        /**
         * The {@link NamedSubqueriesNode} (optional).
         */
        String NAMED_SUBQUERIES = "namedSubqueries";

    }

    /**
     * Deep copy constructor.
     * @param queryRoot
     */
    public QueryRoot(final QueryRoot queryRoot) {
        
        super(queryRoot);
        
    }

    public QueryRoot(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);
        
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
     * The namespace prefix declarations map. This is a {@link Map} with
     * {@link String} keys (prefix) and {@link String} values (the uri
     * associated with that prefix).
     * 
     * @return The namespace prefix declarations map. If this annotation was not
     *         set, then an empty map will be returned. The returned map is
     *         immutable to preserve the general contract for notification on
     *         mutation.
     */
    @SuppressWarnings("unchecked")
    public Map<String, String> getPrefixDecls() {

        final Map<String, String> prefixDecls = (Map<String, String>) getProperty(Annotations.PREFIX_DECLS);

        if (prefixDecls == null)
            return Collections.emptyMap();

        return Collections.unmodifiableMap(prefixDecls);

    }

    /**
     * Set the namespace prefix declarations map. This is a {@link Map} with
     * {@link String} keys (prefix) and {@link String} values (the uri
     * associated with that prefix).
     */
    public void setPrefixDecls(final Map<String, String> prefixDecls) {

        setProperty(Annotations.PREFIX_DECLS, prefixDecls);

    }

//    /**
//     * Return the optional query hints.
//     * 
//     * @see QueryHints
//     */
//    public Properties getQueryHints() {
//        
//        return (Properties) getProperty(Annotations.QUERY_HINTS);
//        
//    }
//    
//    /**
//     * Set the query hints.
//     * 
//     * @param queryHints
//     *            The query hints (may be <code>null</code>).
//     * 
//     * @see QueryHints
//     */
//    public void setQueryHints(final Properties queryHints) {
//
//        setProperty(Annotations.QUERY_HINTS, queryHints);
//        
//    }

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
     * 
     * @see #getNamedSubqueriesNotNull()
     */
    public NamedSubqueriesNode getNamedSubqueries() {
        
        return (NamedSubqueriesNode) getProperty(Annotations.NAMED_SUBQUERIES);
        
    }

    /**
     * Return the node for the named subqueries. If the node does not exist then
     * it is created, set on the {@link QueryRoot} and returned. This helps out
     * with what is otherwise an awkward conditional construction pattern.
     */
    public NamedSubqueriesNode getNamedSubqueriesNotNull() {

        NamedSubqueriesNode tmp = (NamedSubqueriesNode) getProperty(Annotations.NAMED_SUBQUERIES);

        if (tmp == null) {

            tmp = new NamedSubqueriesNode();

            setProperty(Annotations.NAMED_SUBQUERIES, tmp);

        }

        return tmp;
        
    }

    /**
     * Set or clear the named subqueries node.
     * 
     * @param namedSubqueries
     *            The named subqueries not (may be <code>null</code>).
     * 
     * @see #getNamedSubqueriesNotNull()
     */
    public void setNamedSubqueries(final NamedSubqueriesNode namedSubqueries) {

        setProperty(Annotations.NAMED_SUBQUERIES, namedSubqueries);

    }

    public String toString(final int indent) {
        
        final String s = indent(indent);
        
        final StringBuilder sb = new StringBuilder();

//        final String queryString = getQueryString();
//        
//        final Object parseTree = getParseTree();
        
        final Properties queryHints = getQueryHints();

        final Map<String/* prefix */, String/* uri */> prefixDecls = getPrefixDecls();
        
        final DatasetNode dataset = getDataset();

        final NamedSubqueriesNode namedSubqueries = getNamedSubqueries();

//        if (queryString != null) {
//
//            sb.append(s);
//            sb.append(queryString);
//            sb.append("\n");
//
//        }
//        
//        if (parseTree != null) {
//
//            if(parseTree instanceof SimpleNode) {
//
//                // Dump parse tree for sparql.jjt grammar.
//                sb.append(((SimpleNode)parseTree).dump(s));
//                
//            } else {
//            
//                /*
//                 * Dump some other parse tree, assuming it implements toString()
//                 * as pretty print.
//                 */
//                sb.append(s);
//                sb.append(parseTree.toString());
//                sb.append("\n");
//                
//            }
//
//        }
        
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

        if(prefixDecls != null) {

            for (Map.Entry<String, String> e : prefixDecls.entrySet()) {

                sb.append("\n");

                sb.append(s);
                
                sb.append("PREFIX ");
                
                sb.append(e.getKey());
                
                sb.append(": <");
                
                sb.append(e.getValue());
                
                sb.append(">");

            }

        }
        
        if (dataset != null) {

            sb.append(dataset.toString(indent+1));
            
        }

        if (namedSubqueries != null && !namedSubqueries.isEmpty()) {

            sb.append(namedSubqueries.toString(indent));

        }
        
        sb.append(super.toString(indent));
        
        return sb.toString();
        
    }

}
