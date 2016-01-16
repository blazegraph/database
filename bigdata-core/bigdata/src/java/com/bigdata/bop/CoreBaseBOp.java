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
 * Created on Aug 25, 2011
 */

package com.bigdata.bop;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import com.bigdata.btree.Tuple;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;

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
     * Deep copy clone semantics.
     * <p>
     * {@inheritDoc}
     */
    @Override
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

    /**
     * General contract is a short (non-recursive) representation of the
     * {@link BOp}.
     */
    @Override
    public String toShortString() {
        final BOp t = this;
        if (t instanceof IValueExpression<?>
                || t instanceof IValueExpressionNode) {
            /*
             * Note: toShortString() is intercepted for a few bops, mainly those
             * with a pretty simple structure. This delegates to toString() in
             * those cases.
             */
            return t.toString();
        } else {
            final StringBuilder sb = new StringBuilder();
            sb.append(t.getClass().getSimpleName());
            final Integer tid = (Integer) t.getProperty(Annotations.BOP_ID);
            if (tid != null) {
                sb.append("[" + tid + "]");
//            } else {
//                sb.append("@"+t.hashCode());
            }
            return sb.toString();
        }
    }
    
    /**
     * Return a non-recursive representation of the arguments and annotations
     * for this {@link BOp}.
     */
    @Override
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
            if (t == null) {
                sb.append("<null>");
            } else {
                sb.append(t.toShortString());
            }
            nwritten++;
        }
        sb.append(")");
        annotationsToString(sb);
        return sb.toString();

    }
    
    /**
     * Append a name to a string buffer, possibly shortening the name.
     * The current algorithm for name shortening is to take the end of the name
     * after the pen-ultimate '.'.
     * @param sb
     * @param longishName
     */
    protected void shortenName(final StringBuilder sb, final String longishName) {
    	int lastDot = longishName.lastIndexOf('.');
    	if (lastDot != -1) {
    		int lastButOneDot = longishName.lastIndexOf('.', lastDot - 1);
    		sb.append(longishName.substring(lastButOneDot + 1));
    		return;
    	}
    	sb.append(longishName);
    }

    /**
     * Add a string representation of annotations into a string builder.
     * By default this is a non-recursive operation, however
     * subclasses may override {@link #annotationValueToString(StringBuilder, BOp, int)}
     * in order to make this recursive.
     * @param sb
     */
    protected void annotationsToString(final StringBuilder sb) {
        annotationsToString(sb, 0);
    }

    /**
     * Add a string representation of annotations into a string builder.
     * By default this is a non-recursive operation, however
     * subclasses may override {@link #annotationValueToString(StringBuilder, BOp, int)}
     * in order to make this recursive.
     * @param sb
     */
	protected void annotationsToString(final StringBuilder sb, final int indent) {
		final Map<String,Object> annotations = annotations();
        if (!annotations.isEmpty()) {
            sb.append("[");
            boolean first = true;
            for (Map.Entry<String, Object> e : annotations.entrySet()) {
                if (first)
                    sb.append(" ");
                else
                    sb.append(", ");
                final String key = e.getKey();
                final Object val = e.getValue();
                shortenName(sb, key);
                sb.append("=");
                if (val != null && val.getClass().isArray()) {
                    sb.append(Arrays.toString((Object[]) val));
                } else if (key.equals(IPredicate.Annotations.FLAGS)) {
                    sb.append(Tuple.flagString((Integer) val));
                } else if( val instanceof BOp) {
                    annotationValueToString(sb, (BOp)val, indent);
                } else {
                    sb.append(val);
                }
                first = false;
            }
            sb.append("]");
        }
	}

    /**
     * Add a string representation of a BOp annotation value into a string builder.
     * By default this is a non-recursive operation, however
     * subclasses may override and give a recursive definition, which should respect
     * the given indent.
     * @param sb The destination buffer
     * @param val The BOp to serialize
     * @param indent An indent to use if a recursive approach is chosen.
     */
	protected void annotationValueToString(final StringBuilder sb, final BOp val, final int indent) {
		sb.append(val.toString());
	}
    
    @Override
    final public Object getRequiredProperty(final String name) {

        final Object tmp = getProperty(name);

        if (tmp == null)
            throw new IllegalStateException("Required property: " + name
                    + " : " + this.getClass());

        return tmp;
        
    }

    @Override
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
            if (defaultValue.getClass() == Boolean.class) {
                return (T) Boolean.valueOf("" + val);
            }

        }

        return (T) val;

    }

    @Override
    final public int getId() {
        
        return (Integer) getRequiredProperty(Annotations.BOP_ID);
        
    }

    @Override
    final public boolean isController() {

        return getProperty(Annotations.CONTROLLER,
                Annotations.DEFAULT_CONTROLLER);

    }
    
    @Override
    final public BOpEvaluationContext getEvaluationContext() {

        return getProperty(Annotations.EVALUATION_CONTEXT,
                Annotations.DEFAULT_EVALUATION_CONTEXT);

    }

    /**
     * <code>true</code> if all arguments and annotations are the same.
     */
    @Override
    public boolean equals(final Object other) {

        if (this == other)
            return true;

        if (!(other instanceof BOp))
            return false;
        
        if(this.getClass() != other.getClass())
            return false;

        final BOp o = (BOp) other;

        final int arity = arity();

        if (arity != o.arity())
            return false;

        for (int i = 0; i < arity; i++) {

            final BOp x = get(i);

            final BOp y = o.get(i);

            /*
             * X Y same same : continue (includes null == null); null other :
             * return false; !null other : if(!x.equals(y)) return false.
             */
            if (x != y && x != null && !(x.equals(y))) {

                return false;

            }

        }

        return annotationsEqual(o);

    }

    /**
     * Return <code>true</code> iff the annotations of this {@link BOp} and the
     * other {@link BOp} are equals.
     * <p>
     * Note: This method permits override by subclasses with direct access to
     * the maps to be compared.
     * 
     * @see #annotationsEqual(Map, Map)
     */
    protected boolean annotationsEqual(final BOp o) {

        final Map<String, Object> m1 = annotations();
        
        final Map<String, Object> m2 = o.annotations();
        
        return annotationsEqual(m1,m2);
        
    }    
    
    /**
     * Compares two maps. If the value under a key is an array, then uses
     * {@link Arrays#equals(Object[], Object[])} to compare the values rather
     * than {@link Object#equals(Object)}. Without this, two bops having array
     * annotation values which have the same data but different array instances
     * will not compare as equal.
     * 
     * @param m1
     *            One set of annotations.
     * @param m2
     *            Another set of annotations.
     *            
     * @return <code>true</code> iff the annotations have the same data.
     */
    static protected final boolean annotationsEqual(
            final Map<String, Object> m1,//
            final Map<String, Object> m2//
            ) {
        
        if (m1 == m2)
            return true;

        if (m1 != null && m2 == null)
            return false;

        if (m1.size() != m2.size())
            return false;
        
        final Iterator<Map.Entry<String,Object>> itr = m1.entrySet().iterator();
        
        while(itr.hasNext()) {

            final Map.Entry<String,Object> e = itr.next();
            
            final String name = e.getKey();
            
            final Object v1 = e.getValue();
            
            final Object v2 = m2.get(name);
            
            if(v1 == v2)
                continue;

            if (v1 == null || v2 == null)
                return false;

            if (v1.getClass().isArray()) {
                // Arrays.equals(v1,v2).
                if (!v2.getClass().isArray())
                    return false;
                if (!Arrays.equals((Object[]) v1, (Object[]) v2)) {
                    return false;
                }
            } else {
                // Object.equals().
                if(!v1.equals(v2))
                    return false;
            }
            
        }
        
        return true;
        
    }

    /**
     * The hash code is based on the hash of the operands (cached).
     */
    @Override
    public int hashCode() {

        int h = hash;

        if (h == 0) {

            final int n = arity();

            for (int i = 0; i < n; i++) {

                final BOp arg = get(i);
                
                h = 31 * h + (arg == null ? 0 : arg.hashCode());

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
     * <p>
     * Note: The string is capped out after a maximum supported depth.
     * 
     * @param depth
     *            The indentation depth.
     * 
     * @return A string suitable for indent at that height.
     */
    public static String indent(final int depth) {

        if (depth < 0) {

            return "";

        }

        return ws.substring(0, Math.min(ws.length(), depth * 2));

    }

    /**
     * The contract of this method at this level is under-specified.
     * Sub-classes may choose between:
     * 
     * - return a string representation of the object, similar to the use of {@link #toString()}
     * 
     * Or:
     * 
     * - return a pretty-print representation of the object with indent
     * 
     * Note that the former contract may or may not include recursive descent through a tree-like
     * object, whereas the latter almost certainly does.
     * 
     * @param indent
     * @return
     */
	public String toString(int indent) {
		return toString();
	}
	
    private static final transient String ws = "                                                                                                                                                                                                                                                                                                                                                                                                                                    ";

}
