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
 * Created on Mar 29, 2011
 */

package com.bigdata.rdf.sail;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.LangMatches;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.impl.BindingImpl;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Utility class to manage the efficient translation of openrdf {@link Value}s
 * in a {@link TupleExpr} or {@link BindingSet} into the {@link BigdataValue}s
 * used internally by bigdata.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataValueReplacer {

    private final static Logger log = Logger
            .getLogger(BigdataValueReplacer.class);
    
    private final AbstractTripleStore database;
    
    public BigdataValueReplacer(final AbstractTripleStore database) {
        
        if(database == null)
            throw new IllegalArgumentException();
        
        this.database = database;
        
    }
    
    /**
     * Batch resolve and replace all {@link Value} objects stored in variables
     * or in the {@link Dataset} with {@link BigdataValue} objects, which have
     * access to the 64-bit internal term identifier associated with each value
     * in the database.
     * <p>
     * Note: The native rule execution must examine the resulting
     * {@link BigdataValue}s. If any value does not exist in the lexicon then
     * its term identifier will be ZERO (0L). {@link StatementPattern}s with
     * term identifiers of ZERO (0L) WILL NOT match anything in the data and
     * MUST NOT be executed since a ZERO (0L) will be interpreted as a variable!
     * 
     * @return yucky hack, need to return a new dataset and a new binding set.
     *         dataset is [0], binding set is [1]
     */
    public Object[] replaceValues(Dataset dataset,
            final TupleExpr tupleExpr, BindingSet bindings)
            throws SailException {

        /*
         * Resolve the values used by this query.
         * 
         * Note: If any value can not be resolved, then its term identifier
         * will remain ZERO (0L) (aka NULL). Except within OPTIONALs, this
         * indicates that the query CAN NOT be satisfied by the data since
         * one or more required terms are unknown to the database.
         */
        final HashMap<Value, BigdataValue> values = new HashMap<Value, BigdataValue>();
        
        /*
         * The set of variables encountered in the query.
         */
        final Map<String/* name */, Var> vars = new LinkedHashMap<String, Var>();

        final BigdataValueFactory valueFactory = database.getValueFactory();

        if (dataset != null) {

            for(URI uri : dataset.getDefaultGraphs())
                values.put(uri, valueFactory.asValue(uri));
            
            for(URI uri : dataset.getNamedGraphs())
                values.put(uri, valueFactory.asValue(uri));
            
        }

        tupleExpr.visit(new QueryModelVisitorBase<SailException>() {

            @Override
            public void meet(final Var var) {
                
                vars.put(var.getName(), var);
                
                if (var.hasValue()) {

                    final Value val = var.getValue();

                    // add BigdataValue variant of the var's Value.
                    values.put(val, valueFactory.asValue(val));
                    
                }
                
            }
            
            @Override
            public void meet(final ValueConstant constant) {
                
                if (constant.getParentNode() instanceof LangMatches) {
                    /* Don't try to resolve for lang matches.
                     * 
                     * Note: Sesame will sometimes use a Literal to represent
                     * a constant parameter to a function, such as LangMatches.
                     * For such uses, we DO NOT want to attempt to resolve the
                     * Literal against the lexicon.  Instead, it should just be
                     * passed through.  BigdataSailEvaluationStrategy is then
                     * responsible for recognizing cases where the lack of an
                     * IV on a constant is associated with such function calls
                     * rather than indicating that the Value is not known to
                     * the KB. 
                     */
                    return;
                }

                final Value val = constant.getValue();

                // add BigdataValue variant of the var's Value.
                values.put(val, valueFactory.asValue(val));
                
            }
            
        });
        
        if (bindings != null) {
        
            final Iterator<Binding> it = bindings.iterator();
        
            while (it.hasNext()) {
                
                final Binding binding = it.next();
                
                final Value val = binding.getValue();
                
                // add BigdataValue variant of the var's Value.
                values.put(val, valueFactory.asValue(val));
                
            }
            
        }

        /*
         * Batch resolve term identifiers for those BigdataValues.
         * 
         * Note: If any value does not exist in the lexicon then its term
         * identifier will be ZERO (0L).
         */
        {
             
             final BigdataValue[] terms = values.values().toArray(
                    new BigdataValue[] {});

             database.getLexiconRelation().addTerms(terms, terms.length,
                     true/* readOnly */);

        }
        
        /*
         * Replace the values with BigdataValues having their resolve term
         * identifiers.
         */
        tupleExpr.visit(new QueryModelVisitorBase<SailException>() {

            @Override
            public void meet(Var var) {
                
                if (var.hasValue()) {

                    // the Sesame Value object.
                    final Value val = var.getValue();

                    // Lookup the resolve BigdataValue object.
                    final BigdataValue val2 = values.get(val);

                    assert val2 != null : "value not found: "+var.getValue();
                    
                    if (log.isDebugEnabled())
                        log.debug("value: " + val + " : " + val2 + " ("
                                + val2.getIV() + ")");

                    if (val2.getIV() == null) {

                        /*
                         * Since the term identifier is NULL this value is
                         * not known to the kb.
                         */
                        
                        if(log.isInfoEnabled())
                            log.info("Not in knowledge base: " + val2);
                        
                    }
                    
                    // replace the constant in the query.
                    var.setValue(val2);

                }
            }
            
            @Override
            public void meet(ValueConstant constant) {
                
                if (constant.getParentNode() instanceof LangMatches) {
                    /* Note: This is parallel to the meet in the visit
                     * pattern above.
                     */
                   return;
                }

                 // the Sesame Value object.
                final Value val = constant.getValue();

                // Lookup the resolved BigdataValue object.
                final BigdataValue val2 = values.get(val);

                assert val2 != null : "value not found: "+constant.getValue();
                
                if (log.isDebugEnabled())
                    log.debug("value: " + val + " : " + val2 + " ("
                            + val2.getIV() + ")");

                if (val2.getIV() == null) {

                    /*
                     * Since the term identifier is NULL this value is
                     * not known to the kb.
                     */
                    
                    if(log.isInfoEnabled())
                        log.info("Not in knowledge base: " + val2);
                    
                }
                
                // replace the constant in the query.
                constant.setValue(val2);
                
            }
            
        });
        
        if (bindings != null) {
        
            final MapBindingSet bindings2 = new MapBindingSet();
        
            final Iterator<Binding> it = bindings.iterator();
        
            while (it.hasNext()) {
                
                final BindingImpl binding = (BindingImpl) it.next();

                if (!vars.containsKey(binding.getName())) {

                    // Drop bindings which are not used within the query.
                    
                    if (log.isInfoEnabled())
                        log.info("Dropping unused binding: var=" + binding);
                    
                    continue;
                    
                }
                
                final Value val = binding.getValue();
                
                // Lookup the resolved BigdataValue object.
                final BigdataValue val2 = values.get(val);

                assert val2 != null : "value not found: "+binding.getValue();
                
                if (log.isDebugEnabled())
                    log.debug("value: " + val + " : " + val2 + " ("
                            + val2.getIV() + ")");

                if (val2.getIV() == null) {

                    /*
                     * Since the term identifier is NULL this value is
                     * not known to the kb.
                     */
                    
                    if(log.isInfoEnabled())
                        log.info("Not in knowledge base: " + val2);
                    
                }
                
                // replace the constant in the query.
                bindings2.addBinding(binding.getName(), val2);
                
            }
            
            bindings = bindings2;
            
        }

        if (dataset != null) {
            
            final DatasetImpl dataset2 = new DatasetImpl();
            
            for(URI uri : dataset.getDefaultGraphs())
                dataset2.addDefaultGraph((URI)values.get(uri));
            
            for(URI uri : dataset.getNamedGraphs())
                dataset2.addNamedGraph((URI)values.get(uri));
            
            dataset = dataset2;
            
        }
        
        return new Object[] { dataset, bindings };

    }

}
