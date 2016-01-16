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
 * Created on Jul 20, 2015
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.join.FastRangeCountOp;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Interface for an external evaluation of {@link JoinGroupNode}s.
 * 
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public interface IExternalAST2BOp
{
    /**
     * Translate the given join group.
     * 
     * @param left
     * @param joinGroup
     * @param doneSet
     * @param start
     * @param ctx
     */
    PipelineOp convertJoinGroup( final PipelineOp left,
            final JoinGroupNode joinGroup,
            final Set<IVariable<?>> doneSet,
            final AtomicInteger start,
            final AST2BOpContext ctx);

    /**
     * Use a {@link FastRangeCountOp}.
     *
     * @see https://github.com/SYSTAP/bigdata-gpu/issues/101
     */
    PipelineOp fastRangeCountJoin( final PipelineOp left,
                                   final List<NV> anns,
                                   final Predicate<?> pred,
                                   final DatasetNode dataset,
                                   final Long cutoffLimitIsIgnored,
                                   final VarNode fastRangeCountVar,
                                   final Properties queryHints,
                                   final AST2BOpContext ctx );
}
