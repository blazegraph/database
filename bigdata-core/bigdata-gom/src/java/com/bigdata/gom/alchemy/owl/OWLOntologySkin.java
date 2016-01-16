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

import java.util.Iterator;

import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.gom.gpo.BasicSkin;
import com.bigdata.gom.gpo.GPO;
import com.bigdata.gom.gpo.IGPO;
import com.bigdata.gom.gpo.IGenericSkin;
import com.bigdata.gom.om.IObjectManager;

public class OWLOntologySkin extends BasicSkin implements IGenericSkin {

	public OWLOntologySkin(IGPO gpo) {
		super(gpo);
	}

	
	static public OWLOntologySkin getOntology(IObjectManager om) {
		final IGPO owl = om.getGPO(OWL.ONTOLOGY);

		return (OWLOntologySkin) ((GPO) owl).getSkin(OWLOntologySkin.class);
	}

	/**
	 * Returns a list of defined OWLClasses.  The classes do not
	 * in fact have any reference to the Ontology instance, but the
	 * skin supports the fiction.
	 */
	public Iterator<OWLClassSkin> getClasses() {
		final IObjectManager om = m_gpo.getObjectManager();
		
		final IGPO classClass = om.getGPO(OWL.CLASS);
		
		final Iterator<IGPO> owlClasses = classClass.getLinksIn(RDF.TYPE).iterator();
		
		return new Iterator<OWLClassSkin>() {

			@Override
			public boolean hasNext() {
				return owlClasses.hasNext();
			}

			@Override
			public OWLClassSkin next() {
				IGPO nxt = owlClasses.next();
				return (OWLClassSkin) ((GPO) nxt).getSkin(OWLClassSkin.class);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};		
	}
}
