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
 * Created on Oct 3, 2011
 */

package com.bigdata.rdf.lexicon;

import com.bigdata.btree.keys.KVO;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.service.ndx.pipeline.IDuplicateRemover;
import com.bigdata.service.ndx.pipeline.KVOC;
import com.bigdata.service.ndx.pipeline.KVOList;

/**
 * Assigns the term identifier to duplicate {@link BigdataValue} for a single
 * write operation when an {@link IDuplicateRemover} was applied.
 * 
 * @todo this should be more transparent. One way to do that is to get rid of
 *       {@link KVOList#map(com.bigdata.service.ndx.pipeline.KVOList.Op)} and
 *       {@link KVOList.Op} and provide a TERM2ID index write specific interface
 *       extending KVO. When the term identifier is assigned, we then invoke the
 *       method on that interface to set the term identifier on the original and
 *       any duplicates. The method would have to know about {@link KVOList}
 *       however, and that means that we would really need to different
 *       implementations depending on whether {@link KVOC} was being extended or
 *       not. This is possibly even more messy.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AssignTermId implements KVOList.Op<BigdataValue> {

    @SuppressWarnings("rawtypes")
    private final IV iv;

    @SuppressWarnings("rawtypes")
    public AssignTermId(final IV iv) {

        this.iv = iv;

    }

    public void apply(final KVO<BigdataValue> t) {

        t.obj.setIV(iv);

        // System.err.println("Assigned term identifier to duplicate: "+tid+" : "+t.obj);

    }

}
