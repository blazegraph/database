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
package com.bigdata.rdf.lexicon;

import java.util.Map;

import org.openrdf.model.Value;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;

/**
 * A <code>
 * lex(BigdataValue,IV)
 * </code> predicate used for querying the {@link LexiconRelation}'s TERMS
 * index.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class LexPredicate extends Predicate<BigdataValue> {

    /**
     * 
     */
    private static final long serialVersionUID = 6379772624297485704L;

	        /**
     * Simplified forward lookup ctor. Use this ctor to lookup an {@link IV}
     * from a {@link BigdataValue}.
     * 
     * @param relationName
     *            the namespace of the lexicon relation
     * @param timestamp
     *            The timestamp of the view to read on. This should be the same
     *            as the timestamp associated with the view of the triple store
     *            except for a full read/write transaction. Since all writes on
     *            the lexicon are unisolated, a full read/write transaction must
     *            use the {@link ITx#UNISOLATED} view of the lexicon in order to
     *            ensure that any writes it performs will be visible.
     * @param var
     *            The variable on which the {@link IV} will be bound when we
     *            read on the access path.
     * @param term
     *            the term to resolve using forward lookup (term2id)
     */
    public static LexPredicate forwardInstance(//
            final String relationName,//
    		final long timestamp,//
    		final IVariableOrConstant<BigdataValue> term,//
    		final IVariable<IV> var//
    		) {

        return new LexPredicate(
            new IVariableOrConstant[] { 
                term,      // term 
                Var.var(), // iv 
            },
            new NV(Annotations.RELATION_NAME, new String[] { relationName }),
            new NV(Annotations.TIMESTAMP, timestamp) //
        );

    }

	    /**
     * Simplified reverse lookup ctor. Use this ctor to lookup a
     * {@link BigdataValue} from an {@link IV}.
     * 
     * @param relationName
     *            the namespace of the lexicon relation
     * @param timestamp
     *            The timestamp of the view to read on. This should be the same
     *            as the timestamp associated with the view of the triple store
     *            except for a full read/write transaction. Since all writes on
     *            the lexicon are unisolated, a full read/write transaction must
     *            use the {@link ITx#UNISOLATED} view of the lexicon in order to
     *            ensure that any writes it performs will be visible.
     * @param var The
     *            variable on which the {@link Value} will be bound when we read
     *            on the access path.
     * @param term
     *            the term to resolve using reverse lookup (id2term)
     */
    public static LexPredicate reverseInstance(final String relationName,
    		final long timestamp,
    		final IVariable<BigdataValue> var,
    		final IVariableOrConstant<IV> term) {

        return new LexPredicate(//
            new IVariableOrConstant[] {//
                var, // term
                term, // iv
            },
            new NV(Annotations.RELATION_NAME, new String[] { relationName }),
            new NV(Annotations.TIMESTAMP, timestamp) //
        );

    }

    /**
     * Variable argument version of the shallow copy constructor.
     */
    public LexPredicate(final BOp[] args, final NV... anns) {
        super(args, anns);
    }

    /**
     * Required shallow copy constructor.
     */
    public LexPredicate(final BOp[] args, final Map<String, Object> anns) {
        super(args, anns);
    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public LexPredicate(final LexPredicate op) {
        super(op);
    }

    @Override
    public LexPredicate clone() {

        // Fast path for clone().
        return new LexPredicate(this);
        
    }

    /**
     * Return the {@link BigdataValue} at index position
     * {@value LexiconKeyOrder#SLOT_TERM}.
     */
    @SuppressWarnings("unchecked")
    final public IVariableOrConstant<BigdataValue> term() {
        
        return (IVariableOrConstant<BigdataValue>) get(LexiconKeyOrder.SLOT_TERM);
        
    }
    
    /**
     * Return the {@link IV} at index position {@value LexiconKeyOrder#SLOT_IV}.
     */
    @SuppressWarnings("unchecked")
    final public IVariableOrConstant<IV> iv() {
        
        return (IVariableOrConstant<IV>) get(LexiconKeyOrder.SLOT_IV);
        
    }
    
    /**
     * Strengthened return type.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public LexPredicate asBound(final IBindingSet bindingSet) {

        return (LexPredicate) new LexPredicate(argsCopy(), annotationsRef())
                ._asBound(bindingSet);
        
//        return (LexPredicate) super.asBound(bindingSet);

    }

}
