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

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.openrdf.model.URI;

import com.bigdata.rdf.internal.IPrefixedURIHandler;
import com.bigdata.rdf.internal.ISuffixedURIHandler;
import com.bigdata.rdf.internal.InlinePrefixedFixedWidthIntegerURIHandler;
import com.bigdata.rdf.internal.InlinePrefixedIntegerURIHandler;
import com.bigdata.rdf.internal.InlinePrefixedSuffixedIntegerURIHandler;
import com.bigdata.rdf.internal.InlineURIFactory;
import com.bigdata.rdf.internal.InlineURIHandler;
import com.bigdata.rdf.internal.impl.uri.URIExtensionIV;

/**
 * InlineURIFactory for the {@link PubChemVocabularyDecl}
 * 
 * Include by adding the line below to your namespace properties.
 * 
 * com.bigdata.rdf.store.AbstractTripleStore.vocabularyClass=com.blazegraph.
 * vocab.pubchem.
 * com.bigdata.rdf.store.AbstractTripleStore.inlineURIFactory=com.
 * blazegraph.vocab.pubchem.PubChemInlineURIFactory
 * 
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
	public final static String[] uris = {
			"http://rdf.ncbi.nlm.nih.gov/pubchem/compound/", // 1
			"http://rdf.ncbi.nlm.nih.gov/pubchem/substance/", // 2
			"http://rdf.ncbi.nlm.nih.gov/pubchem/bioassay/", // 3
			"http://rdf.ncbi.nlm.nih.gov/pubchem/protein/", // 4
			"http://rdf.ncbi.nlm.nih.gov/pubchem/conserveddomain/", // 5
			"http://rdf.ncbi.nlm.nih.gov/pubchem/gene/", // 6
			"http://rdf.ncbi.nlm.nih.gov/pubchem/biosystem/", // 7
			"http://rdf.ncbi.nlm.nih.gov/pubchem/reference/", // 8
			"http://semanticscience.org/resource/",  // 9
			"http://purl.obolibrary.org/obo/" }; 			///10

	public final static String[] localNames = { "CID", // 1
			"SID", // 2
			"AID", // 3
			"GI", // 4
			"PSSMID", // 5
			"GID", // 6
			"BSID", // 7
			"PMID", // 8
			"CHEMINF_", //9
			"CHEBI_"}; // 10

	/*
	 * http://rdf.ncbi.nlm.nih.gov/pubchem/descriptor/
	 * 
	 * descriptor:CID5606223_Compound_Identifier ,
	 * descriptor:CID5606223_Covalent_Unit_Count ,
	 * descriptor:CID5606223_Defined_Atom_Stereo_Count ,
	 * descriptor:CID5606223_Defined_Bond_Stereo_Count ,
	 * descriptor:CID5606223_Exact_Mass ,
	 * descriptor:CID5606223_Hydrogen_Bond_Acceptor_Count ,
	 * descriptor:CID5606223_Hydrogen_Bond_Donor_Count ,
	 * descriptor:CID5606223_IUPAC_InChI , descriptor:CID5606223_Isomeric_SMILES
	 * , descriptor:CID5606223_Isotope_Atom_Count ,
	 * descriptor:CID5606223_Molecular_Formula ,
	 * descriptor:CID5606223_Molecular_Weight ,
	 * descriptor:CID5606223_Mono_Isotopic_Weight ,
	 * descriptor:CID5606223_Non-hydrogen_Atom_Count ,
	 * descriptor:CID5606223_Preferred_IUPAC_Name ,
	 * descriptor:CID5606223_Rotatable_Bond_Count ,
	 * descriptor:CID5606223_Structure_Complexity , descriptor:CID5606223_TPSA ,
	 * descriptor:CID5606223_Tautomer_Count ,
	 * descriptor:CID5606223_Total_Formal_Charge ,
	 * descriptor:CID5606223_Undefined_Atom_Stereo_Count ,
	 * descriptor:CID5606223_Undefined_Bond_Stereo_Count ,
	 * descriptor:CID5606223_XLogP3-AA .
	 */
	public static final String[] descriptorSuffix = { "_XLogP3-AA",
			"_Undefined_Bond_Stereo_Count", "_Undefined_Atom_Stereo_Count",
			"_Total_Formal_Charge", "_Tautomer_Count", "_TPSA",
			"_Structure_Complexity", "_Rotatable_Bond_Count",
			"_Preferred_IUPAC_Name", "_Non-hydrogen_Atom_Count",
			"_Mono_Isotopic_Weight", "_Molecular_Weight", "_Molecular_Formula",
			"_Isotope_Atom_Count", "_Isomeric_SMILES", "_IUPAC_InChI",
			"_Hydrogen_Bond_Donor_Count", "_Hydrogen_Bond_Acceptor_Count",
			"_Exact_Mass", "_Defined_Bond_Stereo_Count",
			"_Defined_Atom_Stereo_Count", "_Covalent_Unit_Count",
			"_Compound_Identifier" };

	//Precompile
	public static final Pattern descriptorPattern = Pattern.compile("(.*_)(\\d+)(_.*)");

	public static final String descriptorPrefix = "CID_";
	public static final String descriptorNS = "http://rdf.ncbi.nlm.nih.gov/pubchem/descriptor/";

	public PubChemInlineURIFactory() {
		super();

		for (int i = 0; i < uris.length; i++) {
			addHandler(new InlinePrefixedIntegerURIHandler(uris[i],
					localNames[i]));
		}

		// http://www.bioassayontology.org/bao#BAO_0002877 fixed width 7
		addHandler(new InlinePrefixedFixedWidthIntegerURIHandler(
				"http://www.bioassayontology.org/bao#", "BAO_", 7));

		// http://purl.obolibrary.org/obo/PR_000005253 //fixed width 9
		addHandler(new InlinePrefixedFixedWidthIntegerURIHandler(
				"http://purl.obolibrary.org/obo/", "PR_", 9));

		// http://purl.obolibrary.org/obo/IAO_0000136 //fixed width 7
		addHandler(new InlinePrefixedFixedWidthIntegerURIHandler(
				"http://purl.obolibrary.org/obo/", "IAO_", 7));

		// http://purl.obolibrary.org/obo/OBI_0000299 //fixed width 7
		addHandler(new InlinePrefixedFixedWidthIntegerURIHandler(
				"http://purl.obolibrary.org/obo/", "OBI_", 7));

		for (int i = 0; i < descriptorSuffix.length; i++) {
			addHandler(new InlinePrefixedSuffixedIntegerURIHandler(
					descriptorNS, descriptorPrefix, descriptorSuffix[i]));
		}

	}

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public URIExtensionIV createInlineURIIV(final URI uri) {

		String str;

		//Custom pubchem handling for multiple different inline values
		//with the same namespace
		//Only do this for the desriptorNS
		//TODO:  Handle same namespace, different suffix.  push up into InlineURIFactory
		if(uri.getNamespace().equals(descriptorNS)) {

			final String lName = uri.getLocalName();
		    //We know these are in the form prefix_integer_suffix	
			
			Matcher m = descriptorPattern.matcher(lName);
			
			if(m.matches()) {
				str = uri.getNamespace();
				str+=m.group(1);
				str+=m.group(3);
			} else { //drop into default
				str = uri.stringValue();
			}
			
		} else {
			str = uri.stringValue();
		}
		

		// Find handler with longest prefix match LTE the given URI.
		final Map.Entry<String, InlineURIHandler> floorEntry = getHandlersByNamespace()
				.floorEntry(str);

		if (floorEntry == null) {

			// No potentially suitable handler.
			return null;

		}

		final String prefix = floorEntry.getKey();

		/*
		 * Note: the floorEntry is NOT guaranteed to be a prefix. It can also be
		 * strictly LT the probe key. Therefore we must additionally verify here
		 * that the prefix under which the URI handler was registered is a
		 * prefix of the URI before invoking that handler.
		 */
		if (str.startsWith(prefix)) {

			final InlineURIHandler handler = floorEntry.getValue();

			final URIExtensionIV iv = handler.createInlineIV(uri);

			if (iv != null) {

				return iv;

			}

		}

		return null;

	}

	@Override
	/**
	 * Override to allow for prefix and suffix handlers with the same URI, but different suffixes. 
	 * 
	 */
	protected void addHandler(final InlineURIHandler handler) {

		getHandlersByNamespace().put(getKeyForHandler(handler), handler);

	}
	
	private String getKeyForHandler(final InlineURIHandler handler) {
	
		String key = handler.getNamespace();
		
		if(handler instanceof IPrefixedURIHandler) {
			key += ((IPrefixedURIHandler)handler).getPrefix();
		}
		
		if(handler instanceof ISuffixedURIHandler) {
			key += ((ISuffixedURIHandler)handler).getSuffix();
		}
		
		return key;
		
	}
}
