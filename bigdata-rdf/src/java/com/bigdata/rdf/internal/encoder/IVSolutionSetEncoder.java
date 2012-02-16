/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Feb 15, 2012
 */

package com.bigdata.rdf.internal.encoder;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVCache;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueSerializer;

/**
 * This class provides fast, efficient serialization for solution sets. Each
 * solution must be an {@link IBindingSet}s whose bound values are {@link IV}s
 * and their cached {@link BigdataValue}s. The {@link IV}s and the cached
 * {@link BigdataValue}s are efficiently and compactly represented in format
 * suitable for chunked messages or streaming. Decode is a fast online process.
 * Both encode and decode require the maintenance of a map from the {@link IV}
 * having cached {@link BigdataValue}s to those cached values.
 * 
 * <h2>Record Format</h2>
 * 
 * The format is as follows:
 * 
 * <pre>
 * nbound ncached bitmap+ IV[0] ... IV[nbound-1] Value[0] ... Value[ncached-1]
 * </pre>
 * 
 * where <code>nbound</code> is the #of bindings in the binding set. <br/>
 * where <code>ncached</code> is the #of bindings in the binding set for which
 * there is a cached {@link BigdataValue} which has not already been written
 * into a previous record. Even if the {@link IV} has a cached
 * {@link BigdataValue}, if the {@link IV} has been previously written into a
 * record then the {@link IV} is NOT record in this record with a cached Value.
 * Further, if the {@link IV} appears more than once in a given record, the
 * cached value is only marked in the bitmap for the first such occurrence and
 * the cached value is only written into the record once. <br/>
 * where <code>bitmap</code> is one or more bytes providing a bit map indicating
 * which IVs are associated with cached values written into the record. Whether
 * or not an IV has a cached value must be decided by the caller after
 * processing the record and consulting an (IV,Value) cache which they maintain
 * over the set of records processed to date. Cached values are written out (and
 * the bit set) only the first time a given IV with a cached Value is observed. <br/>
 * where <code>IV[n]</code> is an {@link IV} as encoded by {@link IVUtility}. <br/>
 * where {@link BigdataValue} is an RDF Value serialized using the
 * {@link BigdataValueSerializer} for the namespace of the lexicon.
 * 
 * <h2>Decode</h2>
 * 
 * The namespace of the lexicon is required in to obtain the
 * {@link BigdataValueFactory} and {@link BigdataValueSerializer} used to decode
 * and materialize the cached {@link BigdataValue}s. This information can be
 * sent before the records if it is not known to the caller.
 * <p>
 * The decoder materializes the cached values into a map (either a HashMap or
 * HTree, as appropriate for the data scale) as the records are processed. Only
 * one solution needs to be decoded at a time, but the decoder must maintain the
 * (IV,Value) cache across all decoded records. There is no need to indicate the
 * #of records, but IChunkMessage#getSolutionCount() in fact reports exactly
 * that information.
 * <p>
 * Each solution can be turned into an {@link IBindingSet} at the time that it
 * is decoded. If we use a standard {@link ListBindingSet}, then we need to
 * resolve each {@link IV} against the {@link IV} cache, setting its RDF Value
 * as a side effect before returning the IBindingSet to the caller. If we do a
 * custom {@link IBindingSet} implementation, then the cached
 * {@link BigdataValue} could be lazily materialized by hooking
 * {@link IVCache#getValue()}. Either way, the life cycle of the materialized
 * objects will be very short unless they are propagated into new solutions.
 * Short life cycle objects entail very little heap burden.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/475">Optimize
 *      serialization for query messages on cluster </a>
 */
public class IVSolutionSetEncoder {

    // TBD - this is being developed in TestIVSolutionSetEncoder.

}
