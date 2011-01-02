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
 * Created on Dec 31, 2010
 */
package com.bigdata.bop.engine;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.PipelineOp;

/**
 * In order to setup the push/pop of the sink and altSink we need to specify
 * certain metadata about the source groupId, the target groupId, and whether
 * the transition is via the sink or the altSink. The groupId for the source and
 * target operators MAY be null, in which case the operator is understood to be
 * outside of any conditional binding group.
 * <p>
 * The action to be taken when the binding set is written to the sink or the
 * altSink is determined by a simple decision matrix.
 * 
 * <pre>
 *           | toGroup 
 * fromGroup + null + newGroup + sameGroup
 *     null  | NOP  | Push     | n/a
 *    group  | Pop  | Pop+Push | NOP
 * </pre>
 * 
 * The value of the [boolean:save] flag for pop is decided based on whether the
 * transition is via the default sink (save:=true) or the altSink (save:=false).
 * 
 * @see PipelineOp.Annotations#CONDITIONAL_GROUP
 * 
 * @todo Unit tests of this class in isolation.
 * 
 * @todo It appears that this design can not be made to satisfy SPARQL optional
 *       group semantics. Therefore, we may be able to drop this class, support
 *       for it in the {@link ChunkedRunningQuery} and support for the symbol
 *       table stack in {@link IBindingSet}.
 */
class SinkTransitionMetadata {

    private final Integer fromGroupId;

    private final Integer toGroupId;

    private final boolean isSink;

    public String toString() {

        return getClass().getSimpleName() + "{from=" + fromGroupId + ",to="
                + toGroupId + ",isSink=" + isSink + "}";

    }

    public SinkTransitionMetadata(final Integer fromGroupId,
            final Integer toGroupId, final boolean isSink) {

        this.fromGroupId = fromGroupId;

        this.toGroupId = toGroupId;

        this.isSink = isSink;

    }

    /**
     * Apply the appropriate action(s) to the binding set.
     * 
     * @param bset
     *            The binding set.
     */
    public void handleBindingSet(final IBindingSet bset) {
        if (fromGroupId == null) {
            if (toGroupId == null)
                return;
            // Transition from no group to some group.
            bset.push();
            return;
        } else {
            if (toGroupId == null)
                // Transition from a group to no group.
                bset.pop(isSink/* save */);
            else if (toGroupId.equals(fromGroupId)) {
                // NOP (transition to the same group)
            } else {
                // Transition to a different group.
                bset.pop(isSink/* save */);
                bset.push();
            }
        }
    }

}
