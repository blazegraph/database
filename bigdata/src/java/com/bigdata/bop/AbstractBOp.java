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
/*
 * Created on Aug 16, 2010
 */

package com.bigdata.bop;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.bigdata.bop.constraint.EQ;

/**
 * Abstract base class for {@link BOp}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractBOp implements BOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * The argument values.
     * <p>
     * Note: This field is reported out as a {@link List} so we can make it
     * thread safe and, if desired, immutable. However, it is internally a
     * simple array and exposed to subclasses so they can benefit from fast
     * positional access to the arguments in operations which would otherwise
     * become hot, such as {@link EQ#accept(IBindingSet)}.
     * <p>
     * If we allow mutation of the arguments then caching of the arguments (or
     * annotations) by classes such as {@link EQ} will cause {@link #clone()} to
     * fail because (a) it will do a field-by-field copy on the concrete
     * implementation class; and (b) it will not consistently update the cached
     * references. In order to "fix" this problem, any classes which cache
     * arguments or annotations would have to explicitly overrides
     * {@link #clone()} in order to set those fields based on the arguments on
     * the cloned {@link AbstractBOp} class.
     */
    protected final BOp[] args;

    /**
     * The operator annotations.
     */
    protected final Map<String,Object> annotations;
    
    /**
     * Check the operator argument.
     * 
     * @param args
     *            The arguments.
     * 
     * @throws IllegalArgumentException
     *             if the arguments are not valid for the operator.
     */
    protected void checkArgs(final Object[] args) {

    }

    /**
     * Deep copy clone semantics for {@link #args} and {@link #annotations}.
     * <p>
     * {@inheritDoc}
     * 
     * @todo This will deep copy {@link BOp} structures but does not do a deep
     *       copy of other kinds of embedded structures.
     */
    public AbstractBOp clone() {
        try {
            final AbstractBOp tmp = (AbstractBOp) super.clone();
            // deep copy the arguments.
            {
                final int arity = arity();
                for (int i = 0; i < arity; i++) {
                    tmp.args[i] = (BOp) (args[i].clone());
                }
            }
            // deep copy the annotations.
            {
                final Iterator<Map.Entry<String, Object>> itr = annotations
                        .entrySet().iterator();
                while (itr.hasNext()) {
                    final Map.Entry<String, Object> e = itr.next();
                    if (e.getValue() instanceof BOp) {
                        tmp.annotations.put(e.getKey(), ((BOp) e.getValue())
                                .clone());
                    }
                }
            }
            return tmp;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * @param args
     *            The arguments to the operator.
     */
    protected AbstractBOp(final BOp[] args) {
       
        this(args, null/* annotations */);
        
    }

    /**
     * @param args
     *            The arguments to the operator.
     * @param annotations
     *            The annotations for the operator (optional).
     */
    protected AbstractBOp(final BOp[] args,
            final Map<String, Object> annotations) {

        if (args == null)
            throw new IllegalArgumentException();
        
        checkArgs(args);
        
        final ArrayList<BOp> tmp = new ArrayList<BOp>(args.length);
        
        for (int i = 0; i < args.length; i++) {
        
            tmp.add(args[i]);
            
        }
        
        this.args = args;
        
        this.annotations = (annotations == null ? new LinkedHashMap<String, Object>()
                : annotations);
        
    }

    final public Map<String, Object> annotations() {

        return Collections.unmodifiableMap(annotations);
    
    }
    
    public BOp get(final int index) {
        
        return args[index];
        
    }
    
    final public int arity() {
        
        return args.length;
        
    }

    final public List<BOp> args() {

        return Collections.unmodifiableList(Arrays.asList(args));
        
    }

    /**
     * Return the value of the named annotation.
     * 
     * @param name
     *            The name of the annotation.
     * @param defaultValue
     *            The default value.
     * @return The annotation value -or- the <i>defaultValue</i> if the
     *         annotation was not bound.
     * @param <T>
     *            The generic type of the annotation value.
     */
    @SuppressWarnings("unchecked")
    public <T> T getProperty(final String name, final T defaultValue) {

        if (!annotations.containsKey(name))
            return defaultValue;

        return (T) annotations.get(name);

    }

}
