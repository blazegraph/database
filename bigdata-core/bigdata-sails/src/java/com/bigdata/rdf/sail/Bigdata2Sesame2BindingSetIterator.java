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
package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryInterruptedException;
import org.openrdf.query.impl.MapBindingSet;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.engine.QueryTimeoutException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.util.InnerCause;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Converts a bigdata {@link ICloseableIterator} {@link IBindingSet}s containing
 * either (a) {@link BigdataValue}s or (b) {@link IV}s having cached
 * {@link BigdataValue}s into a Sesame 2 {@link CloseableIteration} visiting
 * Sesame 2 {@link BindingSet}s containing {@link BigdataValue}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: Bigdata2Sesame2BindingSetIterator.java 5021 2011-08-04
 *          15:44:21Z thompsonbry $
 * @param <E>
 *            The generic type of the thrown exception.
 */
public class Bigdata2Sesame2BindingSetIterator implements
        CloseableIteration<BindingSet, QueryEvaluationException> {

    final protected static Logger log = Logger
            .getLogger(Bigdata2Sesame2BindingSetIterator.class);

    /**
     * The source iterator (will be closed when this iterator is closed).
     */
    private final ICloseableIterator<IBindingSet> src;
    
    private final BindingSet constants;
    
    private volatile boolean open = true;

    /** Pre-fetched result for {@link #next()}. */
    private BindingSet next = null;
    
    /**
     * 
     * @param src
     *            The source iterator (will be closed when this iterator is
     *            closed). All bound values in the visited {@link IBindingSet}s
     *            MUST be either (a) {@link BigdataValue}s -or- (b) {@link IV}s
     *            having a cached {@link BigdataValue}.
     */
    public Bigdata2Sesame2BindingSetIterator(
            final ICloseableIterator<IBindingSet> src) {

        this(src, null);
    }

    /**
     * 
     * @param src
     *            The source iterator (will be closed when this iterator is
     *            closed). All bound values in the visited {@link IBindingSet}s
     *            MUST be {@link IV}s and those {@link IV}s MUST have their
     *            {@link BigdataValue}s cached.
     * @param constants
     *            Optional constants to be united with the output solutions.
     */
    public Bigdata2Sesame2BindingSetIterator(
            final ICloseableIterator<IBindingSet> src,
            final BindingSet constants) {

        if (src == null)
            throw new IllegalArgumentException();

        this.src = src;
        
        this.constants = constants;

    }
    
    /**
     * {@inheritDoc}
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/503">
     *      Bigdata2Sesame2BindingSetIterator throws QueryEvaluationException
     *      were it should throw NoSuchElementException. </a>
     */
    public boolean hasNext() throws QueryEvaluationException {

        try {

            if (!open) {
                return false;
            }
            if (next != null) {
                // already fetched.
                return true;
            }
            if (!src.hasNext()) {
                // Exhausted.
                close();
                return false;
            }
            final IBindingSet bset = src.next(); // fetch
            next = getBindingSet(bset); // resolve.
            return true;

        } catch (Throwable t) {

            if (!open) {
                /**
                 * The iterator was concurrently closed. This often means that
                 * the connection guarding the query was concurrently closed, in
                 * which case it is possible for a concurrent writer to have
                 * triggered recycling (on the RWStore). Therefore, we want to
                 * ignore any thrown exception after the iterator was closed
                 * since a wide variety of problems could be triggered by
                 * reading against a commit point that had since been recycled.
                 * <p>
                 * Note: The logic to fetch the next result was moved into
                 * hasNext() in order to avoid doing any work in next(). Thus,
                 * if there is any problem resolving the next chunk of
                 * solutions, hasNext() will report [false] if the iterator was
                 * concurrently closed and otherwise will throw out the
                 * exception.
                 * 
                 * @see <a
                 *      href="https://sourceforge.net/apps/trac/bigdata/ticket/644"
                 *      > Bigdata2Sesame2BindingSetIterator can fail to notice
                 *      asynchronous close() </a>
                 */
                return false;
            }
            
            // Ensure closed.
            try {
                close();
            } catch (Throwable t2) {
                // Ignore.
            }
            
            // Wrap and rethrow.
            if (InnerCause.isInnerCause(t, QueryTimeoutException.class)) {
            
                /*
                 * Align with the openrdf API.
                 */
            
                throw new QueryInterruptedException(t);
                
            } else {
                
                throw new QueryEvaluationException(t);
                
            }
            
        }

    }

    public BindingSet next() throws QueryEvaluationException {

        try {
            
            if (!hasNext())
                throw new NoSuchElementException();

            final BindingSet tmp = next;
            
            next = null;
            
            return tmp;
            
        } catch (NoSuchElementException t) {

            /*
             * Note: This exception should not be wrapped per the ticket above.
             */

            throw t;
            
        }
//        } catch (Throwable t) {
//
//            if (!open) {
//                /**
//                 * The iterator was concurrently closed. This often means that
//                 * the connection guarding the query was concurrently closed, in
//                 * which case it is possible for a concurrent writer to have
//                 * triggered recycling (on the RWStore). Therefore, we want to
//                 * ignore any thrown exception after the iterator was closed
//                 * since a wide variety of problems could be triggered by
//                 * reading against a commit point that had since been recycled.
//                 * 
//                 * @see <a
//                 *      href="https://sourceforge.net/apps/trac/bigdata/ticket/644"
//                 *      > Bigdata2Sesame2BindingSetIterator can fail to notice
//                 *      asynchronous close() </a>
//                 */
//                throw new NoSuchElementException();
//            }
//
//            if (InnerCause.isInnerCause(t, QueryTimeoutException.class)) {
//
//                /*
//                 * Align with the openrdf API.
//                 */
//
//                throw new QueryInterruptedException(t);
//
//            } else {
//
//                throw new QueryEvaluationException(t);
//
//            }
//
//        }

    }

    /**
     * Aligns a bigdata {@link IBindingSet} with the Sesame 2 {@link BindingSet}.
     * 
     * @param src
     *            A bigdata {@link IBindingSet} containing only
     *            {@link BigdataValue}s.
     * 
     * @return The corresponding Sesame 2 {@link BindingSet}.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws ClassCastException
     *             if a bound value is not a {@link BigdataValue}.
     */
    @SuppressWarnings("rawtypes")
    private BindingSet getBindingSet(final IBindingSet src) {

        if (src == null)
            throw new IllegalArgumentException();

        final int n = src.size();

        final MapBindingSet bindingSet = new MapBindingSet(n /* capacity */);

        final Iterator<Map.Entry<IVariable,IConstant>> itr = src.iterator();

        while (itr.hasNext()) {

            final Map.Entry<IVariable, IConstant> entry = itr.next();

            final IVariable<?> v = entry.getKey();

            final IConstant<?> c = entry.getValue();
            
            final Object val = c.get();

            // Anonymous variables are not materialized at this point, as there is no need them to be projected,
            // so we neither need to add them to binding set, nor could add them as they don't have associated BigdataValue
            // Ref: Test_Ticket_T168741
            if (v.isAnonymous() && val instanceof IV && !((IV)val).hasValue()) { // Skip adding anonymous vars to binding set
                continue;
            }

            final BigdataValue value;
            if (val instanceof IV) {
                /*
                 * The bound value is an IV. The IV MUST have the BigdataValue
                 * cached.
                 */
                value = ((IV) val).getValue();
            } else {
                // Otherwise the bound value must be a BigdataValue.
                value = (BigdataValue) val;
            }
            
            bindingSet.addBinding(v.getName(), value);
            
        }
        
        if (constants != null) {
            
            final Iterator<Binding> it = constants.iterator();

            while (it.hasNext()) {
            
                final Binding b = it.next();
                
                bindingSet.addBinding(b.getName(), b.getValue());
                
            }
            
        }
        
        return bindingSet;
        
    }
    
    /**
     * @throws UnsupportedOperationException
     */
    public void remove() throws QueryEvaluationException {

        throw new UnsupportedOperationException();

    }

    public void close() throws QueryEvaluationException {

        if (open) {

            open = false;

            src.close();

        }

    }

}
