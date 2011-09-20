/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
/*
 * Created on Sep 1, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import info.aduna.iteration.CloseableIteration;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;

import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.ConstructNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Iterator consumes the solutions from a query and interprets them according to
 * a {@link ConstructNode}. Ground triples in the template are output
 * immediately. Any non-ground triples are output iff they are fully (and
 * validly) bound for a given solution. Blank nodes are scoped to a solution.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ASTConstructIterator.java 5131 2011-09-05 20:48:48Z thompsonbry
 *          $
 * 
 *          TODO Quads extensions for CONSTRUCT template.
 * 
 *          TODO Was there a feature in native construct for the statement type
 *          (inferred, explicit, axioms)?
 */
public class ASTConstructIterator implements
        CloseableIteration<BigdataStatement, QueryEvaluationException> {

    private static final Logger log = Logger
            .getLogger(ASTConstructIterator.class);
    
    private final BigdataValueFactory f;

    /**
     * The non-ground statement patterns.
     */
    private final List<StatementPatternNode> templates;

    private final CloseableIteration<BindingSet, QueryEvaluationException> src;

    /**
     * A list of {@link Statement}s constructed from the most recently visited
     * solution. Statements are drained from this buffer, sending them to the
     * sink. Once the buffer is empty, we will go back to the {@link #src} to
     * refill it.
     * <p>
     * The buffer is pre-populated with any ground triples in the construct
     * template by the constructor.
     */
    private List<BigdataStatement> buffer = new LinkedList<BigdataStatement>();

    /**
     * A factory for blank node identifiers, which are scoped to a solution.
     */
    private int bnodeIdFactory = 0;
    
    private boolean open = true;

    /**
     * 
     */
    public ASTConstructIterator(final AbstractTripleStore store,
            final ConstructNode construct,
            final CloseableIteration<BindingSet, QueryEvaluationException> src) {

        this.f = store.getValueFactory();
        
        final List<StatementPatternNode> templates = new LinkedList<StatementPatternNode>();
        
        for(StatementPatternNode pat : construct) {
            
            if (pat.isGround()) {

                // add statement from template to buffer.
                final BigdataStatement stmt = f.createStatement(//
                        (Resource) pat.s().getValue(),//
                        (URI) pat.p().getValue(),//
                        (Value) pat.o().getValue(),//
                        pat.c() == null ? null : (Resource) pat.c().getValue()//
                        );

                if (log.isInfoEnabled())
                    log.info("Ground statement: pattern" + pat + "\nstmt"
                            + stmt);

                buffer.add(stmt);
                
            } else {
                
                /*
                 * A statement pattern that we will process for each solution.
                 */
                
                templates.add(pat);
                
            }
            
        }
        
        this.templates = templates;

        this.src = src;

    }
    
    public boolean hasNext() throws QueryEvaluationException {

        while (true) {

            if (!buffer.isEmpty()) {
                
                /*
                 * At least one statement is ready in the buffer.
                 */

                return true;
                
            }

            if (!src.hasNext()) {

                /*
                 * Nothing left to visit.
                 */
                
                close();

                return false;

            }

            /*
             * Refill the buffer from the next available solution.
             */
            
            fillBuffer(src.next());

            /*
             * Check to see whether we can assemble any statements from that
             * solution.
             */
            
            continue;
            
        }
        
    }

    @Override
    public BigdataStatement next() throws QueryEvaluationException {

        if (!hasNext())
            throw new NoSuchElementException();

        /*
         * Remove and return the first statement from the buffer.
         */
        
        return buffer.remove(0);
        
    }

    public void close() throws QueryEvaluationException {

        if (open) {
        
            open = false;
            
            src.close();
            
        }

    }

    public void remove() throws QueryEvaluationException {

        throw new UnsupportedOperationException();

    }

    /**
     * Refill the buffer from a new solution. This method is responsible for the
     * scope of blank nodes and for discarding statements which are ill-formed
     * (missing slots, bad type for a slot, etc).
     * 
     * @param solution
     */
    private void fillBuffer(final BindingSet solution) {

        // Should only be invoked when the buffer is empty.
        assert buffer.isEmpty();
        
        // Blank nodes (scoped to the solution).
        final Map<String,BigdataBNode> bnodes = new LinkedHashMap<String, BigdataBNode>();
        
        for(StatementPatternNode pat : templates) {

            /*
             * Attempt to build a statement from this statement pattern and
             * solution.
             */
            
            final BigdataStatement stmt = makeStatement(pat, solution, bnodes);

            if(stmt != null) {
            
                // If successful, then add to the buffer.
                buffer.add(stmt);
                
            }
            
        }
                
    }

    /**
     * Return a statement if a valid statement could be constructed for that
     * statement pattern and this solution.
     * 
     * @param pat
     *            A statement pattern from the construct template.
     * @param solution
     *            A solution from the query.
     * @param bnodes
     *            A map used to scope blank nodes to the solution.
     * 
     * @return A statement if a valid statement could be constructed for that
     *         statement pattern and this solution.
     */
    private BigdataStatement makeStatement(final StatementPatternNode pat,
            final BindingSet solution, final Map<String, BigdataBNode> bnodes) {

        // resolve values from template and/or solution.
        final BigdataValue s = getValue(pat.s(), solution, bnodes);
        final BigdataValue p = getValue(pat.p(), solution, bnodes);
        final BigdataValue o = getValue(pat.o(), solution, bnodes);
        final BigdataValue c = pat.c() == null ? null : getValue(pat.c(),
                solution, bnodes);

        // filter out unbound values.
        if (s == null || p == null || o == null)
            return null;

        // filter out bindings which do not produce legal statements.
        if (!(s instanceof Resource))
            return null;
        if (!(p instanceof URI))
            return null;
        if (!(o instanceof Value))
            return null;
        if (c != null && !(c instanceof Resource))
            return null;

        // return the statement
        return f.createStatement((Resource) s, (URI) p, (Value) o, (Resource) c);
        
    }

    /**
     * Return the as-bound value of the variable or constant given the solution.
     * 
     * @param term
     *            Either a variable or a constant from the statement pattern in
     *            the template.
     * @param solution
     *            A solution from the query.
     * @param bnodes
     *            A map used to scope blank nodes to the solution.
     * 
     * @return The as-bound value.
     */
    private BigdataValue getValue(final TermNode term,
            final BindingSet solution, final Map<String, BigdataBNode> bnodes) {

        if (term instanceof ConstantNode) {

            final BigdataValue value = term.getValue();
            
            if(value instanceof BigdataBNode) {

                return getBNode(((BigdataBNode) value).getID(), bnodes);

            }
            
            return value;

        } else if(term instanceof VarNode) {

            /*
             * I can't quite say whether or not this is a hack, so let me
             * explain what is going on instead. When the SPARQL grammar parses
             * a blank node in a query, it is *always* turned into an anonymous
             * variable. So, when we interpret the CONSTRUCT template, we are
             * going to see anonymous variables and we have to recognize them
             * and treat them as if they were really blank nodes.
             * 
             * The code here tests VarNode.isAnonymous() and, if the variable is
             * anonymous, it uses the variable's *name* as a blank node
             * identifier (ID). It then obtains a unique within scope blank node
             * which is correlated with that blank node ID.
             */
            
            final VarNode v = (VarNode) term;

            final String varname = v.getValueExpression().getName();

            if(v.isAnonymous()) {
                
                return getBNode(varname, bnodes); 
                
            }
            
            return (BigdataValue) solution.getValue(varname);

        } else {
            
            // TODO Support the BNode() function here.
            throw new UnsupportedOperationException("term: "+term);
            
        }
        
    }
    
    /**
     * Scope the bnode ID to the solution. The same ID in each solution is
     * mapped to the same bnode. The same ID in a new solution is mapped to a
     * new BNode.
     */
    private BigdataBNode getBNode(final String id,
            final Map<String, BigdataBNode> bnodes) {

        final BigdataBNode tmp = bnodes.get(id);

        if (tmp != null) {

            // We've already seen this ID for this solution.
            return tmp;

        }

        /*
         * This is the first time we have seen this ID for this solution. We
         * create a new blank node with an identifier which will be unique
         * across the solutions.
         */

        // new bnode, which will be scoped to this solution.
        final BigdataBNode bnode = f.createBNode("b"
                + Integer.valueOf(bnodeIdFactory++).toString());

        // put into the per-solution cache.
        bnodes.put(id, bnode);

        return bnode;

    }
    
}
