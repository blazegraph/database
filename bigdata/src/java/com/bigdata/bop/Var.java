package com.bigdata.bop;

import java.io.ObjectStreamException;
import java.util.UUID;

import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.relation.rule.Rule;

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
 * @todo it is not entirely satisfactory to have variable names disappear from
 *       rules as they are specialized. It might be better to define a BoundVar
 *       (extends Var) whose id was the bound value and which reported true for
 *       both isVariable() and isConstant(). However this would mean that var(x) !=
 *       var(x,id) where the latter is bound to a constant. Or if {@link Var} is
 *       rule-local (or rule execution thread local) then it could define
 *       set(Object) and get():Object to access its binding. A rule that was
 *       specialized would then flag bound variables as immutable.
 * 
 * @todo Variable canonicalization could be limited in scope to a rule or its
 *       derived rules (via specialization or for truth maintenance). This would
 *       make it easier to place constraints directly on the variable so that it
 *       can limit the types of binding that it will accept. Constraints on
 *       {@link IPredicate}s limit the matched tuples. Constraints on rules
 *       limit the binding patterns across the rule.  (In fact, variables can be
 *       "named" by their index into the binding set for most purposes.)
 */
final public class Var<E> extends BOpBase implements IVariable<E>,
        Comparable<IVariable<E>> {

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
    private Var(final String name) {

        super(new BOp[]{});
        
        assert name != null;

        this.name = name;

    }

    /**
     * Clone is overridden to prevent variables from becoming aliased (this is
     * part of the canonicalizing mapping). Because we override clone we do not
     * need to provide the deep copy constructor (it is never invoked).
     */
    final public Var<E> clone() {

        return this;
        
    }

    /**
     * @todo Why two versions of equals? This one is coming from
     *       IConstantOrVariable.
     */
    public final boolean equals(final IVariableOrConstant<E> o) {

        if (this == o)
            return true;

        if (o instanceof IVariable<?>) {

            return name.equals(((IVariable<?>) o).getName());

        }

        return false;

    }

    public final boolean equals(final Object o) {

        if (this == o)
            return true;

        if (o instanceof IVariable<?>) {

            return name.equals(((IVariable<?>) o).getName());

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

    public E get(final IBindingSet bindingSet) {

        if (bindingSet == null)
            throw new IllegalArgumentException();

        @SuppressWarnings("unchecked")
        final IConstant<E> c = bindingSet.get(this);

        return c == null ? null : c.get();

    }
    
    public String getName() {

        return name;

    }

    /**
     * Canonicalizing hash map with weak values for {@link Var}s.
     * 
     * @see ConcurrentWeakValueCache
     */
    static private final ConcurrentWeakValueCache<String, Var<?>> vars = new ConcurrentWeakValueCache<String, Var<?>>(
            0// queue capacity (no hard reference queue).
    );

    /**
     * Generate an anonymous random variable.
     */
    static public Var<?> var() {
        
        return Var.var(UUID.randomUUID().toString());
        
    }
    
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
    static public Var var(final String name) {

        if (name == null)
            throw new IllegalArgumentException();

        if (name.length() == 0)
            throw new IllegalArgumentException();

        Var<?> var = vars.get(name);

        if (var == null) {

            final Var<?> tmp = vars.putIfAbsent(name, var = new Var(name));
             
            if(tmp != null) {
                
                // race condition - someone else inserted first.
                var = tmp;
                
            }
            
//            synchronized (vars) {
//
//                // Only one thread gets to create the variable for that name.
//
//                var = new Var(name);
//
//                vars.put(name, var);
//
//            }

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
