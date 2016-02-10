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
 * Created on Oct 29, 2015
 */

package com.bigdata.rdf.internal;

import java.util.Collection;

import com.bigdata.rdf.internal.impl.extensions.CompressedTimestampExtension;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValue;

/**
 * Extension factory that adds a compressed timestamp literal datatype, namely
 * <http://www.bigdata.com/rdf/datatype#compressedTimestamp>. Integers typed with
 * this datatype should be literals, which are compactly stored in the database.
 * 
 * @author <a href="ms@metaphacts.com">Michael Schmidt</a>
 */
public class CompressedTimestampExtensionFactory extends DefaultExtensionFactory {

    protected void _init(final IDatatypeURIResolver resolver,
            final ILexiconConfiguration<BigdataValue> lex,
            final Collection<IExtension> extensions) {

        extensions.add(new CompressedTimestampExtension<BigdataLiteral>(resolver));

    }
    
}
