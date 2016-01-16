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
 * Created on Jan 2, 2012
 */

package com.bigdata.rdf.lexicon;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;

/**
 * Reduced interface for the {@link LexiconRelation}'s term cache. This
 * interface was added because the term cache reference is actually passed into
 * some helper classes. The interface makes it clear when we are operating on
 * the term cache rather than some other concurrent map.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ITermCache<K extends IV<?, ?>, V extends BigdataValue> {

    int size();
    
    V get(K k);

    V putIfAbsent(K k, V v);

    void clear();
    
}
