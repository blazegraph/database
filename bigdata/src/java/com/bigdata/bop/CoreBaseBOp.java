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
 * Created on Aug 25, 2011
 */

package com.bigdata.bop;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import com.bigdata.btree.Tuple;

/**
 * Base class with some common methods for mutable and copy-on-write {@link BOp}
 * s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class CoreBaseBOp implements BOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * The default initial capacity used for an empty annotation map -- empty
     * maps use the minimum initial capacity to avoid waste since we create a
     * large number of {@link BOp}s during query evaluation.
     */
    static protected transient final int DEFAULT_INITIAL_CAPACITY = 2;

    /**
     * Check the operator argument.
     * 
     * @param args
     *            The arguments.
     * 
     * @throws IllegalArgumentException
     *             if the arguments are not valid for the operator.
     */
    protected void checkArgs(final BOp[] args) {

    }

    /**
     * Deep copy clone semantics for {@link #args} and {@link #annotations}.
     * <p>
     * {@inheritDoc}
     */
    public CoreBaseBOp clone() {
        final Class<? extends CoreBaseBOp> cls = getClass();
        final Constructor<? extends CoreBaseBOp> ctor;
        try {
            ctor = cls.getConstructor(new Class[] { cls });
            return ctor.newInstance(new Object[] { this });
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    public String toString() {
        
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName());
//        sb.append(super.toString());
        final Integer bopId = (Integer) getProperty(Annotations.BOP_ID);
        if (bopId != null) {
            sb.append("[" + bopId + "]");
        }
        sb.append("(");
        int nwritten = 0;
        final Iterator<BOp> itr = argIterator();
        while(itr.hasNext()) {
            final BOp t = itr.next();
            if (nwritten > 0)
                sb.append(',');
            if(t instanceof IValueExpression<?>) {
                sb.append(t.toString());
            } else {
                sb.append(t.getClass().getSimpleName());
                final Integer tid = (Integer) t.getProperty(Annotations.BOP_ID);
                if (tid != null) {
                    sb.append("[" + tid + "]");
                }
            }
            nwritten++;
        }
        sb.append(")");
        annotationsToString(sb);
        return sb.toString();

    }

    protected void annotationsToString(final StringBuilder sb) {
        final Map<String,Object> annotations = annotations();
        if (!annotations.isEmpty()) {
            sb.append("[");
            boolean first = true;
            for (Map.Entry<String, Object> e : annotations.entrySet()) {
                if (first)
                    sb.append(" ");
                else
                    sb.append(", ");
                if (e.getValue() != null && e.getValue().getClass().isArray()) {
                    sb.append(e.getKey() + "="
                            + Arrays.toString((Object[]) e.getValue()));
                } else if (e.getKey().equals(IPredicate.Annotations.FLAGS)) {
                    sb.append(e.getKey() + "="
                            + Tuple.flagString((Integer) e.getValue()));
                } else {
                    sb.append(e.getKey() + "=" + e.getValue());
                }
                first = false;
            }
            sb.append("]");
        }
    }
    
    final public Object getRequiredProperty(final String name) {

        final Object tmp = getProperty(name);

        if (tmp == null)
            throw new IllegalStateException("Required property: " + name
                    + " : " + this);

        return tmp;
        
    }

    @SuppressWarnings("unchecked")
    final public <T> T getProperty(final String name, final T defaultValue) {

        final Object val = getProperty(name);

        if (val == null)
            return defaultValue;

        if (defaultValue != null && val.getClass() != defaultValue.getClass()) {

            /*
             * Attempt to convert to the correct target type.
             */
            
            if (defaultValue.getClass() == Integer.class) {
                return (T) Integer.valueOf("" + val);
            }
            if (defaultValue.getClass() == Long.class) {
                return (T) Long.valueOf("" + val);
            }
            if (defaultValue.getClass() == Float.class) {
                return (T) Float.valueOf("" + val);
            }
            if (defaultValue.getClass() == Double.class) {
                return (T) Double.valueOf("" + val);
            }

        }

        return (T) val;

    }

    final public int getId() {
        
        return (Integer) getRequiredProperty(Annotations.BOP_ID);
        
    }
        
    final public boolean isController() {
        
        return getProperty(Annotations.CONTROLLER, false);
        
    }
    
    final public BOpEvaluationContext getEvaluationContext() {

        return getProperty(Annotations.EVALUATION_CONTEXT,
                Annotations.DEFAULT_EVALUATION_CONTEXT);

    }

    /**
     * <code>true</code> if all arguments and annotations are the same.
     */
    public boolean equals(final Object other) {

        if (this == other)
            return true;

        if (!(other instanceof BOp))
            return false;

        final BOp o = (BOp) other;

        final int arity = arity();

        if (arity != o.arity())
            return false;

        for (int i = 0; i < arity; i++) {

            final BOp x = get(i);

            final BOp y = o.get(i);

            /*
             *    X      Y
             * same   same : continue (includes null == null);
             * null  other : return false;
             * !null other : if(!x.equals(y)) return false.
             */
            if (x != y && x != null && !(x.equals(y))) {

                return false;

            }

        }

        return annotations().equals(o.annotations());

    }

    /**
     * The hash code is based on the hash of the operands (cached).
     */
    public int hashCode() {

        int h = hash;

        if (h == 0) {

            final int n = arity();

            for (int i = 0; i < n; i++) {

                h = 31 * h + get(i).hashCode();

            }

            hash = h;

        }

        return h;

    }

    /**
     * Caches the hash code.
     */
    private int hash = 0;

    /**
     * Returns a string that may be used to indent a dump of the nodes in the
     * tree.
     * 
     * @param depth
     *            The indentation depth.
     * 
     * @return A string suitable for indent at that height.
     */
    protected static String indent(final int depth) {

        if (depth < 0) {

            return "";

        }

        return ws.substring(0, depth *2);

    }

    private static final transient String ws = "                                                                                                                                                                                                                  ";

}
