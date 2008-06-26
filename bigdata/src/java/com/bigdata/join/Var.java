package com.bigdata.join;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A variable.
 * <p>
 * Note: This implementation provides reference testing for equality. The rest
 * of the package <em>assumes</em> that it can use reference testing for
 * equality when comparing variables.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Variable canonicalization could be limited in scope to a rule or its
 *       derived rules (via specialization or for truth maintenance). This would
 *       make it easier to place constraints directly on the variable so that it
 *       can limit the types of binding that it will accept. Constraints on
 *       {@link IPredicate}s limit the matched tuples. Constraints on rules
 *       limit the binding patterns across the rule.
 */
final public class Var<E> implements IVariable<E>, Comparable<IVariable<E>>, Serializable 
{

    private static final long serialVersionUID = -7100443208125002485L;
    
    final private String name;

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
     */
    static private final Map<String, Var> vars = new HashMap<String, Var>();

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

    /**
     * Orders variables alphabetically.
     */
    public int compareTo(IVariable<E> o) {

        return name.compareTo(o.getName());

    }

//    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
//
//        name = in.readUTF();
//        
//    }
//
//    public void writeExternal(ObjectOutput out) throws IOException {
//
//        out.writeUTF(name);
//        
//    }

    /**
     * Imposes the canonicalizing mapping during object de-serialization.
     */
    private Object readResolve() throws ObjectStreamException {
        
        return var(name);
        
    }
    
}
