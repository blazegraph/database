/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Apr 16, 2008
 */

package com.bigdata.rdf.model;

import org.openrdf.model.Value;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.TempTripleStore;

/**
 * An interface which exposes the internal 64-bit long integer identifiers for
 * {@link Value}s stored within a {@link IRawTripleStore}.  Values may also be
 * stored inline inside the statement indices rather than referencing the 
 * lexicon.  See {@link IV}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface BigdataValue extends Value {//, Comparable<BigdataValue> {

	/**
	 * Return the factory which produced this object. This is guaranteed to be a
	 * singleton (there will only be one {@link BigdataValueFactory} instance
	 * for on a given JVM for all {@link BigdataValue}s associated with a given
	 * lexicon relation namespace).
	 */
    public BigdataValueFactory getValueFactory();
    
    /**
     * Return the internal value for this value. May be a term identifier or an
     * inline value. The term identifier uniquely identifies a {@link Value} 
     * for a database. Sometimes a {@link TempTripleStore} will be used that 
     * shares the lexicon with a given database, in which case the same term 
     * identifiers will be value for that {@link TempTripleStore}.
     */
    public IV getIV();
    
    /**
     * Set the internal value for this value.  May be a term identifier or an
     * inline value.
     * 
     * @param iv
     *            The internal value.
     * 
     * @throws IllegalArgumentException
     *             if <i>iv</i> is null.
     * @throws IllegalStateException
     *             if the internal value is already set to a different non-null
     *             value.
     */
    public void setIV(IV iv);
    
    /**
     * Clears the internal value to null.
     */
    public void clearInternalValue();
    
}
