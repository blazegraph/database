/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
package com.blazegraph.vocab.pubchem;

import com.bigdata.rdf.internal.InlineFixedWidthIntegerURIHandler;
import com.bigdata.rdf.internal.InlinePrefixedFixedWidthIntegerURIHandler;
import com.bigdata.rdf.internal.InlinePrefixedIntegerURIHandler;
import com.bigdata.rdf.internal.InlineURIFactory;

/**
 * InlineURIFactory for the {@link PubChemVocabularyDecl}
 * 
 * Include by adding the line below to your namespace properties.
 * 
 * com.bigdata.rdf.store.AbstractTripleStore.vocabularyClass=com.blazegraph.vocab.pubchem.
com.bigdata.rdf.store.AbstractTripleStore.inlineURIFactory=com.blazegraph.vocab.pubchem.PubChemInlineURIFactory

 * 
 * @author beebs
 *
 */
public class PubChemInlineURIFactory extends InlineURIFactory {

	/*
	 * See https://pubchem.ncbi.nlm.nih.gov/rdf/
	 * 
	 * http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID60823
	 * http://rdf.ncbi.nlm.nih.gov/pubchem/substance/SID8032774
	 * http://rdf.ncbi.nlm.nih.gov/pubchem/bioassay/AID1788
	 * http://rdf.ncbi.nlm.nih.gov/pubchem/protein/GI124375976
	 * http://rdf.ncbi.nlm.nih.gov/pubchem/conserveddomain/PSSMID132758
	 * http://rdf.ncbi.nlm.nih.gov/pubchem/gene/GID367
	 * http://rdf.ncbi.nlm.nih.gov/pubchem/biosystem/BSID82991
	 * http://rdf.ncbi.nlm.nih.gov/pubchem/reference/PMID10395478
	 */

	/*
	 * http://purl.obolibrary.org/obo/CHEBI_74763
	 * http://purl.obolibrary.org/obo/PR_
	 * http://www.bioassayontology.org/bao#BAO_0002877
	 * http://semanticscience.org/resource/CHEMINF_
	 * http://purl.obolibrary.org/obo/IAO_0000136
	 * http://purl.obolibrary.org/obo/OBI_0000299
	 */

	/*
	 * http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID
	 * http://rdf.ncbi.nlm.nih.gov/pubchem/substance/SID
	 * http://rdf.ncbi.nlm.nih.gov/pubchem/bioassay/AID
	 * http://rdf.ncbi.nlm.nih.gov/pubchem/protein/GI
	 * http://rdf.ncbi.nlm.nih.gov/pubchem/conserveddomain/PSSMID
	 * http://rdf.ncbi.nlm.nih.gov/pubchem/gene/GID
	 * http://rdf.ncbi.nlm.nih.gov/pubchem/biosystem/BSID
	 * http://rdf.ncbi.nlm.nih.gov/pubchem/reference/PMID
	 * http://purl.obolibrary.org/obo/CHEBI_74763
	 * http://semanticscience.org/resource/CHEMINF_
	 * 
	 * Fixed width 
	 * 
	 * http://www.bioassayontology.org/bao#BAO_0002877 fixed width 7
	 * http://purl.obolibrary.org/obo/PR_000005253 //fixed width 9
	 * http://purl.obolibrary.org/obo/IAO_0000136 //fixed width 7
	 * http://purl.obolibrary.org/obo/OBI_0000299 //fixed width 7
	 */
	private final static String[] uris = {
			"http://rdf.ncbi.nlm.nih.gov/pubchem/compound/",  		// 1
			"http://rdf.ncbi.nlm.nih.gov/pubchem/substance/", 		// 2
			"http://rdf.ncbi.nlm.nih.gov/pubchem/bioassay/",  		// 3
			"http://rdf.ncbi.nlm.nih.gov/pubchem/protein/",   		// 4
			"http://rdf.ncbi.nlm.nih.gov/pubchem/conserveddomain/", // 5
			"http://rdf.ncbi.nlm.nih.gov/pubchem/gene/",  			// 6
			"http://rdf.ncbi.nlm.nih.gov/pubchem/biosystem/",		// 7
			"http://rdf.ncbi.nlm.nih.gov/pubchem/reference/",       // 8
			"http://purl.obolibrary.org/obo/", //CHEBI				// 9
			"http://semanticscience.org/resource/" };			 	// 10

	private final static String[] localNames = { 
			"CID",													// 1 
			"SID",													// 2 
			"AID", 													// 3
			"GI",													// 4
			"PSSMID",												// 5 
			"GID", 													// 6
			"BSID", 												// 7
			"PMID", 												// 8
			"CHEBI_", 												// 9
			"CHEMINF_" };											// 10
	

	public PubChemInlineURIFactory() {
		super();
		
		for(int i = 0 ; i < uris.length ; i++)
		{
			addHandler(new InlinePrefixedIntegerURIHandler(uris[i], localNames[i]));
		}
		
		 // http://www.bioassayontology.org/bao#BAO_0002877 fixed width 7
		addHandler( new InlinePrefixedFixedWidthIntegerURIHandler("http://www.bioassayontology.org/bao#","BAO_",7));

		// http://purl.obolibrary.org/obo/PR_000005253 //fixed width 9
		addHandler( new InlinePrefixedFixedWidthIntegerURIHandler("http://purl.obolibrary.org/obo/","PR_",9));

		// http://purl.obolibrary.org/obo/IAO_0000136 //fixed width 7
		addHandler( new InlinePrefixedFixedWidthIntegerURIHandler("http://purl.obolibrary.org/obo/","IAO_",7));

		// http://purl.obolibrary.org/obo/OBI_0000299 //fixed width 7
		addHandler( new InlinePrefixedFixedWidthIntegerURIHandler("http://purl.obolibrary.org/obo/","OBI_",7));


	}
}
