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
package com.bigdata.gom.alchemy.owl;

import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.gom.gpo.BasicSkin;
import com.bigdata.gom.gpo.IGPO;
import com.bigdata.gom.gpo.IGenericSkin;

/** 
 * The OWLClassSkin returns Iterator<OWLPropertySkin> with the
 * getProperties method using the RDFS.DOMAIN URI.
 * 
 * So a property has a domain, an RDF.TYPE and an RDFS.RANGE that
 * defines the datatype of the property of an instance.
 * 
 * A confusion is the RDF.TYPE which is one of the main following types:
 * 
 * DatatypeProperty, ObjectProperty, FunctionalProperty
 * 
 * (there are also a few more inverse/transitive etc)
 * 
 * It seems to me that FunctionalProperty is slightly problematic since
 * it appears to be used to indicate 0 or 1 data values.  But also
 * often means unique.
 * 
 * There also seems to be an oddity around whether a property is, say,
 * an owl:datatypeProperty with rdf:type owl:functionalProperty or an
 * owl:functionalProperty with rdf:type owl.datatypeProperty.
 * 
 * It all looks very odd.
 * 
 * The inverseOf property is interesting, but the example from univ-bench
 * doesn't look like an true inverse to me, more like an alias.
 * 
 * Alias, or equivalent properties
 * 
 * <owl:ObjectProperty rdf:ID="degreeFrom">
 *   <rdfs:label>has a degree from</rdfs:label>
 *   <rdfs:domain rdf:resource="#Person" />
 *   <rdfs:range rdf:resource="#University" />
 *   <owl:inverseOf rdf:resource="#hasAlumnus"/>
 * </owl:ObjectProperty>
 * 
 * <owl:ObjectProperty rdf:ID="hasAlumnus">
 *   <rdfs:label>has as an alumnus</rdfs:label>
 *   <rdfs:domain rdf:resource="#University" />
 *   <rdfs:range rdf:resource="#Person" />
 *   <owl:inverseOf rdf:resource="#degreeFrom"/>
 * </owl:ObjectProperty>
 * 
 * The inverse of "degreeFrom" should be "degreeTo".  The idea *should* be
 * to normalize the data, to allow the assertion of <uni, degreeTo student>
 * and transform to <student, degreeFrom, uni>.  Not to support both
 * types of statements.  ...and of course to transform queries that
 * use the inverse predicate.
 * 
 * So, to clarify, there should *not* be two properties defined, just one
 * with an "inverseOf" annotation.
 * 
 * @author Martyn Cutcher
 *
 */
public class OWLPropertySkin extends BasicSkin implements IGenericSkin {

	public OWLPropertySkin(IGPO gpo) {
		super(gpo);
	}

	public String getName() {
		return m_gpo.getId().stringValue();
	}

	public boolean isAssociation() {
		return m_gpo.getValue(RDF.TYPE) == OWL.OBJECTPROPERTY;
	}

	/**
	 * Note that this can be null.  The univ-bench.owl does not define
	 * types of Literal values.
	 * 
	 * @return type of property
	 */
	public IGPO getType() {
		return this.getGPOValue(RDFS.RANGE);
	}
}
