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
 * Created on Mar 10, 2012
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Iterator;
import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.rdf.spo.ISPO;

/**
 * Recursive container for ground {@link StatementPatternNode}s. This is used
 * for {@link InsertData} and {@link DeleteData}. It gets flattened out and
 * turned into an {@link ISPO}[].
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class QuadData extends AbstractStatementContainer<IStatementContainer>
        implements IStatementContainer {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public QuadData() {
        
    }
    
    /**
     * @param op
     */
    public QuadData(final QuadData op) {
        
        super(op);
        
    }

    /**
     * @param args
     * @param anns
     */
    public QuadData(final BOp[] args, final Map<String, Object> anns) {
        
        super(args, anns);
        
    }

    public QuadData(final IStatementContainer child) {

        this(new BOp[]{(BOp) child}, null);
        
    }
    
    /**
     * Flatten the {@link QuadData} into a simple {@link ConstructNode}. The
     * {@link ConstructNode} MAY use variables as well as constants and supports
     * the context position, so this is really a quads construct template.
     * 
     * @param template
     *            The {@link ConstructNode} for the template.
     * 
     * @return The argument.
     * 
     *         TODO Maybe we could just flatten this in UpdateExprBuilder?
     */
    public ConstructNode flatten(final ConstructNode template) {

        final QuadData quadData = this;

        final Iterator<StatementPatternNode> itr = BOpUtility.visitAll(
                quadData, StatementPatternNode.class);

        while (itr.hasNext()) {

            final StatementPatternNode sp = (StatementPatternNode) (itr.next()
                    .clone());

            template.addChild(sp);

        }

        return template;

    }
    
    /**
     * Flatten the {@link StatementPatternNode}s into the caller's
     * {@link JoinGroupNode}.
     * 
     * @param container
     *            The caller's container.
     *            
     * @return The caller's container.
     */
    public JoinGroupNode flatten(final JoinGroupNode container) {

        final QuadData quadData = this;

        final Iterator<StatementPatternNode> itr = BOpUtility.visitAll(
                quadData, StatementPatternNode.class);

        while (itr.hasNext()) {

            final StatementPatternNode sp = (StatementPatternNode) (itr.next()
                    .clone());

//            if (sp.getScope() == Scope.NAMED_CONTEXTS) {
//                
//                /*
//                 * Statement pattern must be in a GRAPH group.
//                 * 
//                 * TODO We should coalesce statement patterns which are observed
//                 * to be in the same graph group (same constant or the same
//                 * variable). Note that the lexical scope is not an issue for
//                 * the QuadsData - there are no nested sub-groups or
//                 * sub-selects.
//                 */
//
//                container.addChild(new JoinGroupNode(sp.getContext(), sp));
//                
//            } else {
//                
//                container.addChild(sp);
//             
//            }

            container.addChild(sp);

        }

        return container;

    }

    @Override
    public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder();

        final String s = indent(indent);
        
        sb.append("\n").append(s).append("QUADS {");

        int i = 0;

        for (IStatementContainer v : this) {
            
            if (i >= 10) {
                
                /**
                 * Truncate the description.
                 * 
                 * Note: People sometimes push a LOT of data through with a
                 * DeleteInsertWhere operation. This truncates the description
                 * to avoid problems with log files or the echo of the update
                 * operations in a request by the NSS.
                 * 
                 * @see https://sourceforge.net/apps/trac/bigdata/ticket/613
                 *      (SPARQL UPDATE response inlines large DELETE or INSERT
                 *      triple graphs)
                 * 
                 * @see https ://sourceforge.net/projects/bigdata/forums/forum
                 *      /676946/topic/6092294/index/page/1
                 */
                
                sb.append("... out of " + arity() + " elements\n");

                // Break out of the loop.
                break;
                
            }
            
            sb.append(v.toString(indent + 1));
            
            i++;

        }

        sb.append("\n").append(s).append("}");
        
        return sb.toString();

    }

}
