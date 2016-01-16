/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.impl.BindingImpl;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.query.impl.MapBindingSet;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataOpenRDFBindingSetsResolverator;

/**
 * Utility class to manage the efficient translation of openrdf {@link Value}s
 * in a {@link BindingSet} into the {@link BigdataValue}s used internally by
 * bigdata.
 * 
 * @see BigdataOpenRDFBindingSetsResolverator
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: BigdataValueReplacer.java 5159 2011-09-08 15:35:15Z thompsonbry
 *          $
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
     * its term identifier will be ZERO (0L). Term identifiers of ZERO (0L) WILL
     * NOT match anything in the data and MUST NOT be executed since a ZERO (0L)
     * will be interpreted as a variable!
     * 
     * @return yucky hack, need to return a new dataset and a new binding set.
     *         dataset is [0], binding set is [1]
     */
    public Object[] replaceValues(//
            final Dataset dataset,//
            final BindingSet[] bindingSets//
    ) {

        if(dataset == null && bindingSets == null) {

            // At least one argument must be non-null.
            throw new IllegalArgumentException();
            
        }
        
        /*
         * Resolve the values used by this query.
         * 
         * Note: If any value can not be resolved, then its term identifier
         * will remain ZERO (0L) (aka NULL). Except within OPTIONALs, this
         * indicates that the query CAN NOT be satisfied by the data since
         * one or more required terms are unknown to the database.
         */
        final Map<Value, BigdataValue> values = new LinkedHashMap<Value, BigdataValue>();

        final BigdataValueFactory valueFactory = database.getValueFactory();

        if (dataset != null) {

            for(URI uri : dataset.getDefaultGraphs())
                if (uri != null)
                    values.put(uri, valueFactory.asValue(uri));

            for(URI uri : dataset.getNamedGraphs())
                if (uri != null)
                    values.put(uri, valueFactory.asValue(uri));
            
        }

        if (bindingSets != null && bindingSets.length > 0) {
        
            for(BindingSet bindings : bindingSets) {

                final Iterator<Binding> it = bindings.iterator();

                while (it.hasNext()) {

                    final Binding binding = it.next();

                    final Value val = binding.getValue();

                    // add BigdataValue variant of the var's Value.
                    values.put(val, valueFactory.asValue(val));

                }

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

			database.getLexiconRelation()
					.addTerms(terms, terms.length, true/* readOnly */);

			// cache the BigdataValues on the IVs for later
			for (BigdataValue term : terms) {

				@SuppressWarnings("rawtypes")
                final IV iv = term.getIV();

				if (iv == null) {

					/*
					 * Since the term identifier is NULL this value is not known
					 * to the kb.
					 */

					if (log.isInfoEnabled())
						log.info("Not in knowledge base: " + term);

					/*
					 * Create a dummy iv and cache the unknown value on it so
					 * that it can be used during query evalution.
					 */
					@SuppressWarnings("rawtypes")
                    final IV dummy = TermId.mockIV(VTE.valueOf(term));

					term.setIV(dummy);

					dummy.setValue(term);

				} else {

					iv.setValue(term);

				}

			}

        }
                
        final BindingSet[] bindingSets2;
        
        if (bindingSets == null) {
        
            bindingSets2 = null;
            
        } else {
        
            // Allocate the output solutions array.
            bindingSets2 = new BindingSet[bindingSets.length];
            
            // Generate each output solution from each source solution.
            for (int i = 0; i < bindingSets.length; i++) {

                /*
                 * Replace the bindings with one's which have their IV set.
                 */
        
                final MapBindingSet bindings2 = new MapBindingSet();

                bindingSets2[i] = bindings2; // save off result.
                
                final Iterator<Binding> it = bindingSets[i].iterator();
            
                while (it.hasNext()) {
                    
                    final BindingImpl binding = (BindingImpl) it.next();

                    /*
                     * Note: We can not do this. The Sesame APIs let bindings
                     * flow through the query even if they are not projected by
                     * the query.
                     */
//                if (!vars.containsKey(binding.getName())) {
//
//                    // Drop bindings which are not used within the query.
//                    
//                    if (log.isInfoEnabled())
//                        log.info("Dropping unused binding: var=" + binding);
//                    
//                    continue;
//                    
//                }
                
                    final Value val = binding.getValue();

                    // Lookup the resolved BigdataValue object.
                    final BigdataValue val2 = values.get(val);

                    assert val2 != null : "value not found: "
                            + binding.getValue();

                    if (log.isDebugEnabled())
                        log.debug("value: " + val + " : " + val2 + " ("
                                + val2.getIV() + ")");

//                if (val2.getIV() == null) {
//
//                    /*
//                     * Since the term identifier is NULL this value is not known
//                     * to the kb.
//                     */
//
//                    if (log.isInfoEnabled())
//                        log.info("Not in knowledge base: " + val2);
//
//                }

				/*
				 * This is no good. We need to create a mock IV and also cache
				 * the unknown value on it so that it can be used in filter
				 * evaluation. I took care of this above.
				 */
//                if(val2.getIV() == null) {
//                    /*
//                     * The Value is not in the database, so assign it a mock IV.
//                     * This IV will not match anything during query. However, we
//                     * can not simply fail the query since an OPTIONAL or UNION
//                     * might have solutions even though this Value is not known.
//                     */
//                    val2.setIV(TermId.mockIV(VTE.valueOf(val)));
//                }

                    // rewrite the constant in the query.
                    bindings2.addBinding(binding.getName(), val2);

                }

            }
            
        }

        /*
         * Create a new Dataset using the resolved Values.
         */
        final DatasetImpl dataset2;
        
        if (dataset == null) {

            dataset2 = null;

        } else {

            dataset2 = new DatasetImpl();

            for (URI uri : dataset.getDefaultGraphs())
                dataset2.addDefaultGraph((URI) values.get(uri));

            for (URI uri : dataset.getNamedGraphs())
                dataset2.addNamedGraph((URI) values.get(uri));

        }

        return new Object[] { dataset2, bindingSets2 };

    }

}
