package com.bigdata.join;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A variable.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo variable canonicalization must be maintained during de-serialization.
 *       this matters when we start serializing predicates for remote execution.
 */
final public class Var<E> implements IVariableOrConstant<E> {

    public final String name;

    final public boolean isVar() {

        return true;

    }

    final public boolean isConstant() {

        return false;

    }

    /**
     * Private constructor - use {@link Rule#var(String)} to obtain an instance.
     * 
     * @param name
     */
    private Var(String name) {

        assert name != null;

        this.name = name;

    }

    public final boolean equals(IVariableOrConstant o) {

        if (this == o)
            return true;

        if (o.isVar()) {

            return name.equals(o.getName());

        }

        return false;

    }

    public final int hashCode() {

        return name.hashCode();

    }

    public String toString() {

        return name;

    }

    //        public int compareTo(IVariableOrConstant arg0) {
    //
    //            // order vars before ids
    //            if(arg0.isConstant()) return -1;
    //            
    //            return name.compareTo(((Var)arg0).name);
    //            
    //        }

    public E get() {

        throw new UnsupportedOperationException();

    }

    public String getName() {

        return name;

    }

    /**
     * Canonicalizing map for {@link Var}s.
     * 
     * @todo variables will be serialized for remote joins since they occur in
     *       the {@link IPredicate}. De-serialization must maintain a
     *       canonicalizing mapping!
     */
    static private final Map<String, Var> vars = new ConcurrentHashMap<String, Var>();

    /**
     * Singleton factory for {@link Var}s.
     * <p>
     * Note: While only a single instance of a variable object will be created
     * for any given variable name, the "scope" of the variable is always
     * constrained by the rule within which it is used. The purpose of the
     * singleton factory is to let us test for the same variable using "=="
     * (reference testing) and also to have a shorthand for variable creation.
     * 
     * @param name
     *            The variable name.
     * 
     * @return The singleton variable for that name.
     */
    static public Var var(String name) {

        if (name == null)
            throw new IllegalArgumentException();

        if (name.length() == 0)
            throw new IllegalArgumentException();

        Var var = vars.get(name);

        if (var == null) {

            synchronized (vars) {

                // Only one thread gets to create the variable for that name.

                var = new Var(name);

                vars.put(name, var);

            }

        }

        return var;

    }

}
