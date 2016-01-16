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
 * Created on Mar 1, 2012
 */

package com.bigdata.rdf.sparql.ast.service;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.URI;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVCache;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.BigdataValueReplacer;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Helper class for {@link ServiceCall} invocations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ServiceCallUtility {

    private static final String ERR_NOT_BOUND = "Service reference variable is not bound";

    private static final String ERR_NOT_URI = "Service reference does not evaluate to a URI";

    private static final String ERR_NOT_MATERIALIZED = "Service reference is not materialized";
    
    /**
     * Return the effective service URI IFF the value expression for the service
     * reference is a constant.
     * 
     * @return The effective service URI IFF it is a constant and otherwise
     *         <code>null</code>.
     * 
     * @throws RuntimeException
     *             if the service reference is a constant but does not evaluate
     *             to a {@link URI}.
     * @throws NotMaterializedException
     *             if the service reference evaluates to an {@link IV} which is
     *             not materialized.
     */
    static public BigdataURI getConstantServiceURI(
            final IVariableOrConstant<?> serviceRef) {

        if (serviceRef.isVar()) {

            return null;

        }

        @SuppressWarnings("rawtypes")
        final IV<?, ?> serviceRefIV = (IV) ((IConstant) serviceRef).get();

        if (!serviceRefIV.isURI())
            throw new RuntimeException(ERR_NOT_URI);

        if (!serviceRefIV.hasValue())
            throw new NotMaterializedException(ERR_NOT_MATERIALIZED);

        final BigdataURI serviceURI = (BigdataURI) serviceRefIV.getValue();

        return serviceURI;

    }
    
    /**
     * Return the effective service URI.
     * 
     * @param bset
     *            A solution which will be used to evaluate the service
     *            reference value expression.
     * 
     * @return
     * 
     * @throws RuntimeException
     *             if the service reference is not bound.
     * @throws RuntimeException
     *             if the service reference does not evaluate to a {@link URI}.
     * @throws NotMaterializedException
     *             if the service reference evaluates to an {@link IV} which is
     *             not materialized.
     */
    static public BigdataURI getServiceURI(
            final IVariableOrConstant<?> serviceRef, final IBindingSet bset) {

        // Evaluate the serviceRef expression.
        @SuppressWarnings("rawtypes")
        final IV<?, ?> serviceRefIV = (IV) serviceRef.get(bset);

        if (serviceRefIV == null)
            throw new RuntimeException(ERR_NOT_BOUND);

        if (!serviceRefIV.isURI())
            throw new RuntimeException(ERR_NOT_URI);

        if (!serviceRefIV.hasValue())
            throw new NotMaterializedException(ERR_NOT_MATERIALIZED);

        final BigdataURI serviceURI = (BigdataURI) serviceRefIV.getValue();

        return serviceURI;

    }

    /**
     * Convert the {@link IBindingSet} into an openrdf {@link BindingSet}.
     * <p>
     * Note: The {@link IVCache} MUST be set for non-inline {@link IV}s. 
     * 
     * @param vars
     *            The set of variables which are to be projected (optional).
     *            When given, only the projected variables are in the returned
     *            {@link BindingSet}.
     * @param in
     *            A bigdata {@link IBindingSet} with materialized values.
     *            
     * @throws NotMaterializedException
     *             if a non-inline {@link IV} has not had its {@link IVCache}
     *             set.
     */
    static public BindingSet bigdata2Openrdf(final LexiconRelation lex,
            final Set<IVariable<?>> vars, final IBindingSet in) {

        final MapBindingSet out = new MapBindingSet();
        
        @SuppressWarnings("rawtypes")
        final Iterator<Map.Entry<IVariable,IConstant>> itr = in.iterator();
        
        while(itr.hasNext()) {
        
            @SuppressWarnings("rawtypes")
            final Map.Entry<IVariable,IConstant> e = itr.next();

            final IVariable<?> var = e.getKey();
            
            if (vars != null && !vars.contains(var)) {

                // This variable is not being projected.
                continue;
                
            }
            
            final String name = var.getName();

            @SuppressWarnings("rawtypes")
            final IV iv = (IV) e.getValue().get();

            final BigdataValue value;

            if (iv.isInline()) {

                /**
                 * Materialize inline IV as Value.
                 * 
                 * @see <a
                 *      href="http://sourceforge.net/apps/trac/bigdata/ticket/632">
                 *      NotMaterializedException when a SERVICE call needs
                 *      variables that are provided as query input bindings </a>
                 */
                value = iv.asValue(lex);

            } else {

                try {

                    // Recover Value from the IVCache.
                    value = iv.getValue();

                } catch (NotMaterializedException ex) {

                    /*
                     * Add the variable name to the stack trace.
                     */

                    throw new NotMaterializedException("var=" + name + ", val="
                            + iv, ex);

                }

            }

            out.addBinding(name, value);

        }

        return out;

    }
    
    /**
     * Convert an openrdf {@link BindingSet} into a bigdata {@link IBindingSet}.
     * The {@link BindingSet} MUST contain {@link BigdataValue}s and the
     * {@link IV}s for those {@link BigdataValue}s MUST have been resolved
     * against the database and the {@link IVCache} association set.
     * 
     * @param vars
     *            The variables to be projected (optional). When given, only the
     *            projected variables are in the returned {@link IBindingSet}.
     * @param in
     *            The openrdf {@link BindingSet}
     * 
     * @return The bigdata {@link IBindingSet}.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    static private IBindingSet openrdf2Bigdata(//
            final Set<IVariable<?>> vars,//
            final BindingSet in//
            ) {

        final IBindingSet out = new ListBindingSet();
        
        final Iterator<Binding> itr = in.iterator();
        
        while(itr.hasNext()) {
        
            final Binding e = itr.next();

            final String name = e.getName();

            final IVariable<?> var = Var.var(name);
            
            if (vars != null && !vars.contains(var)) {

                // This variable is not being projected.
                continue;

            }

            // Note: MUST already be BigdataValues.
            final BigdataValue value = (BigdataValue) e.getValue();

            // Note: IVs MUST already be resolved.
            final IV<?,?> iv = value.getIV();
            
            if(iv == null)
                throw new AssertionError();

            // IV must have cached Value.
            if (!iv.hasValue())
                throw new AssertionError();
            
            // The cached Value must be the Value (objects point at each other)
            if(iv.getValue() != value)
                throw new AssertionError();
            
            out.set(var, new Constant(iv));

        }
        
        return out;

    }

    /**
     * Convert {@link IBindingSet}[] to openrdf {@link BindingSet}[].
     * 
     * @param projectedVars
     *            When given, variables which are not projected will not be
     *            present in the returned solutions (optional).
     * @param in
     *            The solutions to be converted (required).
     */
    static public BindingSet[] convert(final LexiconRelation lex,
            final Set<IVariable<?>> projectedVars, final IBindingSet[] in) {

        final BindingSet[] out = new BindingSet[in.length];

        for (int i = 0; i < in.length; i++) {

            out[i] = ServiceCallUtility.bigdata2Openrdf(lex, projectedVars,
                    in[i]);

        }
        
        return out;
        
    }
    
    /**
     * Batch resolve BigdataValues to IVs. This is necessary in order to have
     * subsequent JOINs succeed when they join on variables which are bound to
     * terms which are in the lexicon.
     * <p>
     * Note: This will be a distributed operation on a cluster.
     */
    static public IBindingSet[] resolve(final AbstractTripleStore db,
            final BindingSet[] serviceResults) {

        final BindingSet[] resolvedServiceResults;
        {

            final Object[] b = new BigdataValueReplacer(db).replaceValues(
                    null/* dataset */, serviceResults/* bindings */);

            resolvedServiceResults = (BindingSet[]) b[1];

        }

        /*
         * Convert the openrdf BindingSet[] into a bigdata IBindingSet[].
         */
        final IBindingSet[] bigdataSolutions = new IBindingSet[resolvedServiceResults.length];
        {

            for (int i = 0; i < resolvedServiceResults.length; i++) {
                
                final BindingSet bset = resolvedServiceResults[i];
            
                final IBindingSet bset2 = openrdf2Bigdata(
                        null/* projectedVars */, bset);

                bigdataSolutions[i] = bset2;
                
            }
            
        }

        return bigdataSolutions;
        
    }
    
}
