/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.rdf.magic;

import java.util.Map;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.internal.IV;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.ISolutionExpander;

/**
 * A predicate that is a triple with one or more variables. While the general
 * case allows a predicate to have an arbitrary name, for RDFS reasoning we are
 * only concerned with predicates of the form <code>triple(s,p,o)</code>.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated The generic {@link Predicate} class should be used instead.
 */
public class MagicPredicate extends Predicate<IMagicTuple> {

    protected static final Logger log = Logger.getLogger(MagicPredicate.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = 1396017399712849975L;

    /**
     * Required shallow copy constructor.
     */
    public MagicPredicate(final BOp[] values, final Map<String, Object> annotations) {
        super(values, annotations);
    }

    /**
     * Required deep copy constructor.
     */
    public MagicPredicate(final MagicPredicate op) {
        super(op);
    }

    /**
     * Partly specified ctor. No constraint is specified. No expander is 
     * specified.
     * 
     * @param relationName
     * @param predicateName
     * @param terms
     */
    public MagicPredicate(String relationName, IVariableOrConstant<IV>... terms) {

        this(new String[] { relationName }, -1/* partitionId */,
                null/* constraint */, null/* expander */, terms);

    }

    /**
     * Partly specified ctor. No constraint is specified.
     * 
     * @param relationName
     * @param expander
     *            MAY be <code>null</code>.
     * @param predicateName
     * @param terms
     */
    public MagicPredicate(String relationName, ISolutionExpander expander, 
            IVariableOrConstant<IV>... terms) {

        this(new String[] { relationName }, -1/* partitionId */,
                null/* constraint */, expander,
                terms);

    }

    /**
     * Fully specified ctor.
     * 
     * @param relationName
     * @param partitionId
     * @param constraint
     *            MAY be <code>null</code>.
     * @param expander
     *            MAY be <code>null</code>.
     * @param predicateName
     * @param terms
     */
    public MagicPredicate(String[] relationName, //
            final int partitionId, //
            IElementFilter constraint,//
            ISolutionExpander expander,//
            IVariableOrConstant<IV>... terms//
            ) {
        
        super(terms, relationName[0], partitionId, false, constraint, expander, ITx.READ_COMMITTED);
        
    }

}
