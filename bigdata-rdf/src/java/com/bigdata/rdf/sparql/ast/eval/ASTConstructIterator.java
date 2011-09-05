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

import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.ConstructNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Iterator consumes the solutions from a query and interprets them according to
 * a {@link ConstructNode}. Ground triples in the template are output
 * immediately. Any non-ground triples are output iff they are fully (and
 * validly) bound for a given solution. Blank nodes are scoped to a solution.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
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
                @SuppressWarnings("rawtypes")
                final BigdataStatement stmt = f.createStatement(//
                    (Resource)((IV) pat.s().getValueExpression().get()).getValue(),//
                    (URI)((IV) pat.p().getValueExpression().get()).getValue(),//
                    (Value)((IV) pat.o().getValueExpression().get()).getValue(),//
                    pat.c() == null ? null : (Resource) ((IV) pat.c()
                            .getValueExpression().get()).getValue()//
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
     * 
     *            FIXME BNode() in the CONSTRUCT template.
     * 
     *            FIXME Blank nodes are scoped to a solution.
     * 
     *            TODO Quads extensions for CONSTRUCT template.
     */
    private void fillBuffer(final BindingSet solution) {

        // Should only be invoked when the buffer is empty.
        assert buffer.isEmpty();
        
        for(StatementPatternNode pat : templates) {

//            final (Resource)((IV) pat.s().getValueExpression()).getValue(),//
//            (URI)((IV) pat.p().getValueExpression()).getValue(),//
//            (Value)((IV) pat.o().getValueExpression()).getValue(),//
//            (Resource)((IV) pat.c().getValueExpression()).getValue()//
//
//            
//            final BigdataStatement stmt = f.createStatement(s, p, o, c);
//
//            buffer.add(stmt);

        }
        
        throw new UnsupportedOperationException();
        
    }

}
