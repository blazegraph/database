/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on May 19, 2008
 */

package com.bigdata.gis;

/**
 * <p>
 * This is a placeholder for a GIS design. While GIS systems are typically
 * R-trees, I am considering an approach that leverages the existing scale-out
 * B+-Tree architecture using a design similar in many ways to the triple store.
 * </p>
 * <p>
 * There would be a feature dictionary with a foward and reverse map assigning
 * feature identifiers. The feature identifiers should problably reserve the
 * lower 2-3 bits for coarse coding of broad feature categories so that they may
 * be filtered on those categories without having to resolve them against the
 * feature dictionary - filtering on the feature dictionary is a scattered read.
 * </p>
 * <p>
 * The core indices would use keys formed as <code>lat, lon, alt, feature</code>.
 * The feature needs to show up in the key since there can be more than one
 * feature at the same spatial location. Alternative indices would provide for
 * different access paths for the same key, but the feature would always appear
 * in the last key position giving 3 indices (unless scan of all locations for a
 * feature is common in which case this gives 6 indices, much like a quad
 * store).
 * </p>
 * <p>
 * The value for the coordinate indices could either be unusued or could be a
 * row identifier, much like the sid of the triple store, for making metadata
 * statements about a specific {coordinate,feature} tuple. Other flags might
 * also be carried on the value, but note that the value is replicated on each
 * coordinate access path.
 * </p>
 * <p>
 * If coordinates are entered into the database as of their "event" time then
 * this design would make it easy to obtain the set of spatial features for a
 * given moment in time using a historical read for that event time. However,
 * this is likely to prove difficult when aggregating historical data so the
 * temporary dimension may also need to enter into the key:
 * <code>lat, lon, alt, time, feature</code>
 * </p>
 * <p>
 * I am assuming that latitude, longitude, altitude, and time can all be 64-bit
 * long values assigned from a standard representation such as decimal degrees,
 * meters above/below sea-level, and UTC milliseconds. Equally the (X,Y,Z) could
 * be a polar coordinate system. This is really one level above the data model.
 * Regardless, and unlike the triple store, this means that most of the tuple is
 * directly generated from "natural" units without the aid of a dictionary. Only
 * the feature identifiers are assigned by a dictionary.
 * </p>
 * <p>
 * Features need to be extensible metadata that can be used to link together the
 * spatial extent of both stationary and moving "objects" (tracks). Features
 * could doubtless be related together in additional ways, e.g., through
 * part-whole relations, either within the GIS or in an external metadata
 * database.
 * </p>
 * <p>
 * The basic queries would be things such as "Find me all features F of category
 * X within REGION (X,Y,Z) between TIME (T1,T2)." This would be coded as a JOIN
 * of the various coordinate indices with a filter (where appropriate) on the
 * bits coding coarse feature categories within the identifier and a scatter
 * query doing fine-grained filtering against the reverse map of the feature
 * dictionary.
 * </p>
 * 
 * @todo impl. sketch using refactor of the triple store.
 * 
 * @todo comparison with R-tree based approaches and practical GIS designs.
 * 
 * @todo consideration to metadata mashups.
 * 
 * @todo consideration to data sources and visualization tool chains for demo
 *       purposes.
 * 
 * @todo consideration to reconcilation of feature identity and spatial-temporal
 *       locator error for reporting of tracks.
 * 
 * @todo consideration to very high concurrency for read/write, including
 *       "consistent" writes, read-behind from consistent database states, and
 *       large #ofs tracks (10^6) with no less than 60 seconds update periods.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class GISStore {

}
