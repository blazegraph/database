package com.bigdata.perf.chem2bio2rdf;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import com.bigdata.rdf.vocab.VocabularyDecl;

public class MyVocabularyDecl implements VocabularyDecl {
	static private final URI[] uris = new URI[] {
			new URIImpl("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), // rank=0,
																			// count=6891184
			new URIImpl("http://www.w3.org/2000/01/rdf-schema#label"), // rank=1,
																		// count=6886236
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/activity_id"), // rank=2,
																				// count=4410904
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/molregno"), // rank=3,
																				// count=4344588
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/doc_id"), // rank=4,
																			// count=4235787
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/assay_id"), // rank=5,
																				// count=4006064
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/record_id"), // rank=6,
																				// count=3708427
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/standard_flag"), // rank=7,
																					// count=2972946
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/standard_type"), // rank=8,
																					// count=2972386
			new URIImpl(
					"http://chem2bio2rdf.org/chembl/resource/published_activity_type"), // rank=9,
																						// count=2881032
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/relation"), // rank=10,
																				// count=2732545
			new URIImpl(
					"http://chem2bio2rdf.org/chembl/resource/published_value"), // rank=11,
																				// count=2709724
			new URIImpl(
					"http://chem2bio2rdf.org/chembl/resource/standard_value"), // rank=12,
																				// count=2709563
			new URIImpl(
					"http://chem2bio2rdf.org/chembl/resource/published_units"), // rank=13,
																				// count=2350572
			new URIImpl(
					"http://chem2bio2rdf.org/chembl/resource/standard_units"), // rank=14,
																				// count=2304812
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/cid"), // rank=15,
																		// count=2034500
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/gene"), // rank=16,
																			// count=1442571
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/chemogenomics"), // rank=17,
																					// count=1437870
			new URIImpl(
					"http://chem2bio2rdf.org/chembl/resource/cid_gene_activities"), // rank=18,
																					// count=1437870
			new URIImpl(
					"http://chem2bio2rdf.org/chembl/resource/med_chem_friendly"), // rank=19,
																					// count=1188530
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/compound_name"), // rank=20,
																					// count=735393
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/compound_key"), // rank=21,
																					// count=735334
			new URIImpl(
					"http://chem2bio2rdf.org/chembl/resource/src_description"), // rank=22,
																				// count=732999
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/downgraded"), // rank=23,
																				// count=636267
			new URIImpl(
					"http://chem2bio2rdf.org/chembl/resource/standard_inchi_key"), // rank=24,
																					// count=635931
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/molformula"), // rank=25,
																				// count=635931
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/chebi"), // rank=26,
																			// count=635931
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/inchi_key"), // rank=27,
																				// count=635931
			new URIImpl(
					"http://chem2bio2rdf.org/chembl/resource/canonical_smiles"), // rank=28,
																					// count=635931
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/molweight"), // rank=29,
																				// count=635931
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/inchi"), // rank=30,
																			// count=635926
			new URIImpl(
					"http://chem2bio2rdf.org/chembl/resource/standard_inchi"), // rank=31,
																				// count=635926
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/mw_freebase"), // rank=32,
																				// count=629960
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/ro3_pass"), // rank=33,
																				// count=594265
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/rtb"), // rank=34,
																		// count=594265
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/hbd"), // rank=35,
																		// count=594265
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/hba"), // rank=36,
																		// count=594265
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/alogp"), // rank=37,
																			// count=594265
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/psa"), // rank=38,
																		// count=594265
			new URIImpl(
					"http://chem2bio2rdf.org/chembl/resource/num_ro5_violations"), // rank=39,
																					// count=594265
			new URIImpl(
					"http://chem2bio2rdf.org/chembl/resource/molecular_species"), // rank=40,
																					// count=593827
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/acd_logp"), // rank=41,
																				// count=593274
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/acd_logd"), // rank=42,
																				// count=591362
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/tid"), // rank=43,
																		// count=556631
			new URIImpl(
					"http://chem2bio2rdf.org/chembl/resource/confidence_score"), // rank=44,
																					// count=544132
			new URIImpl(
					"http://chem2bio2rdf.org/chembl/resource/relationship_type"), // rank=45,
																					// count=544132
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/complex"), // rank=46,
																			// count=544132
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/multi"), // rank=47,
																			// count=544132
			new URIImpl(
					"http://chem2bio2rdf.org/chembl/resource/relationship_type_description"), // rank=48,
																								// count=544132
			new URIImpl(
					"http://chem2bio2rdf.org/chembl/resource/confidence_score_description"), // rank=49,
																								// count=544132
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/description"), // rank=50,
																				// count=495995
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/assay_type"), // rank=51,
																				// count=488898
			new URIImpl(
					"http://chem2bio2rdf.org/chembl/resource/assay_type_desc"), // rank=52,
																				// count=488898
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/assay_tax_id"), // rank=53,
																					// count=429013
			new URIImpl(
					"http://chem2bio2rdf.org/chembl/resource/assay_organism"), // rank=54,
																				// count=427806
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/acd_most_apka"), // rank=55,
																					// count=352801
			new URIImpl(
					"http://chem2bio2rdf.org/chembl/resource/activity_comment"), // rank=56,
																					// count=233890
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/generalReference"), // rank=57,
																									// count=72359
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/goClassificationFunction"), // rank=58,
																											// count=72232
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/goClassificationProcess"), // rank=59,
																											// count=63520
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/synonym"), // rank=60,
																							// count=44949
			new URIImpl("http://www.w3.org/2002/07/owl#sameAs"), // rank=61,
																	// count=44247
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/assay_strain"), // rank=62,
																					// count=42264
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/volume"), // rank=63,
																			// count=38461
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/journal"), // rank=64,
																			// count=38461
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/year"), // rank=65,
																			// count=38461
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/last_page"), // rank=66,
																				// count=38461
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/first_page"), // rank=67,
																				// count=38461
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/issue"), // rank=68,
																			// count=38461
			new URIImpl("http://xmlns.com/foaf/0.1/page/homepage"), // rank=69,
																	// count=34726
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/pubmed_id"), // rank=70,
																				// count=34726
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/cellularLocation"), // rank=71,
																									// count=26258
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/drugReference"), // rank=72,
																									// count=23782
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/synonyms"), // rank=73,
																				// count=19356
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/pfamDomainFunction"), // rank=74,
																										// count=19028
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/pfamDomainFunctionPage"), // rank=75,
																											// count=19028
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/brandName"), // rank=76,
																								// count=18166
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/transmembraneRegions"), // rank=77,
																										// count=16115
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/syn_type"), // rank=78,
																				// count=14447
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/goClassificationComponent"), // rank=79,
																												// count=14208
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/target"), // rank=80,
																							// count=12045
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/pref_name"), // rank=81,
																				// count=10801
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/drugType"), // rank=82,
																							// count=10195
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/interactionDrug1"), // rank=83,
																									// count=10153
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/interactionDrug2"), // rank=84,
																									// count=10153
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/text"), // rank=85,
																						// count=10153
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/possibleDiseaseTarget"), // rank=86,
																											// count=8201
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/target_type"), // rank=87,
																				// count=8088
			new URIImpl(
					"http://chem2bio2rdf.org/chembl/resource/target_type_desc"), // rank=88,
																					// count=8088
			new URIImpl("http://xmlns.com/foaf/0.1/page"), // rank=89,
															// count=7912
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/organism"), // rank=90,
																				// count=7625
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/tax_id"), // rank=91,
																			// count=7625
			new URIImpl(
					"http://chem2bio2rdf.org/chembl/resource/protein_sequence"), // rank=92,
																					// count=4909
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/uniprot"), // rank=93,
																			// count=4909
			new URIImpl(
					"http://chem2bio2rdf.org/chembl/resource/protein_md5sum"), // rank=94,
																				// count=4909
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/db_source"), // rank=95,
																				// count=4909
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/db_version"), // rank=96,
																				// count=4899
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/keywords"), // rank=97,
																				// count=4824
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/creationDate"), // rank=98,
																								// count=4772
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/updateDate"), // rank=99,
																								// count=4772
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/massSpecFile"), // rank=100,
																								// count=4772
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/structure"), // rank=101,
																								// count=4772
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/state"), // rank=102,
																							// count=4772
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/genericName"), // rank=103,
																								// count=4772
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/limsDrugId"), // rank=104,
																								// count=4772
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/primaryAccessionNo"), // rank=105,
																										// count=4772
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/molecularWeightAverage"), // rank=106,
																											// count=4727
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/chemicalFormula"), // rank=107,
																									// count=4721
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/gene_names"), // rank=108,
																				// count=4701
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/swissprotPage"), // rank=109,
																									// count=4660
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/swissprotId"), // rank=110,
																								// count=4660
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/molecularWeightMono"), // rank=111,
																										// count=4620
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/swissprotName"), // rank=112,
																									// count=4607
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/name"), // rank=113,
																						// count=4606
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/drugCategory"), // rank=114,
																								// count=4602
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/smilesStringIsomeric"), // rank=115,
																										// count=4600
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/smilesStringCanonical"), // rank=116,
																											// count=4600
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/chemicalIupacName"), // rank=117,
																										// count=4592
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/proteinSequence"), // rank=118,
																									// count=4587
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/numberOfResidues"), // rank=119,
																									// count=4553
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/essentiality"), // rank=120,
																								// count=4543
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/molecularWeight"), // rank=121,
																									// count=4537
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/theoreticalPi"), // rank=122,
																									// count=4532
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/predictedWaterSolubility"), // rank=123,
																											// count=4506
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/predictedLogpHydrophobicity"), // rank=124,
																												// count=4506
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/predictedLogs"), // rank=125,
																									// count=4506
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/pubchemCompoundId"), // rank=126,
																										// count=4433
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/tc_id"), // rank=127,
																			// count=4411
			new URIImpl(
					"http://chem2bio2rdf.org/chembl/resource/target_classification"), // rank=128,
																						// count=4411
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/l1"), // rank=129,
																		// count=4411
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/pubchemSubstanceId"), // rank=130,
																										// count=4400
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/inchiKey"), // rank=131,
																							// count=4392
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/inchiIdentifier"), // rank=132,
																									// count=4392
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/secondaryAccessionNumber"), // rank=133,
																											// count=4364
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/genbankIdGenePage"), // rank=134,
																										// count=4310
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/genbankIdGene"), // rank=135,
																									// count=4310
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/geneSequence"), // rank=136,
																								// count=4231
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/doi"), // rank=137,
																		// count=4205
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/generalFunction"), // rank=138,
																									// count=4103
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/brandMixture"), // rank=139,
																								// count=3968
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/geneName"), // rank=140,
																							// count=3923
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/specificFunction"), // rank=141,
																									// count=3686
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/pdbIdPage"), // rank=142,
																								// count=3379
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/pdbId"), // rank=143,
																							// count=3379
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/hetId"), // rank=144,
																							// count=3312
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/chebi_par_id"), // rank=145,
																					// count=3236
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/pdbExperimentalId"), // rank=146,
																										// count=3113
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/signal"), // rank=147,
																							// count=2890
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/genbankIdProteinPage"), // rank=148,
																										// count=2859
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/genbankIdProtein"), // rank=149,
																									// count=2859
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/ec_number"), // rank=150,
																				// count=2806
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/max_phase"), // rank=151,
																				// count=2757
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/dosageForm"), // rank=152,
																								// count=2482
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/reference"), // rank=153,
																								// count=2362
			new URIImpl(
					"http://chem2bio2rdf.org/chembl/resource/therapeutic_flag"), // rank=154,
																					// count=2305
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/casRegistryNumber"), // rank=155,
																										// count=2240
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/l2"), // rank=156,
																		// count=2039
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/l3"), // rank=157,
																		// count=1985
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/l4"), // rank=158,
																		// count=1983
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/l5"), // rank=159,
																		// count=1901
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/atcCode"), // rank=160,
																							// count=1900
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/description"), // rank=161,
																								// count=1861
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/reaction"), // rank=162,
																							// count=1762
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/hgncId"), // rank=163,
																							// count=1675
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/hgncIdPage"), // rank=164,
																								// count=1675
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/genatlasId"), // rank=165,
																								// count=1665
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/locus"), // rank=166,
																							// count=1619
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/chromosomeLocation"), // rank=167,
																										// count=1614
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/brandedDrug"), // rank=168,
																								// count=1593
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/bio2rdfSymbol"), // rank=169,
																									// count=1533
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/genecardId"), // rank=170,
																								// count=1533
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/affectedOrganism"), // rank=171,
																									// count=1531
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/mechanismOfAction"), // rank=172,
																										// count=1475
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/indication"), // rank=173,
																								// count=1463
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/pharmacology"), // rank=174,
																								// count=1443
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/cell_line"), // rank=175,
																				// count=1393
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/experimentalLogpHydrophobicity"), // rank=176,
																													// count=1370
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/keggCompoundId"), // rank=177,
																									// count=1331
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/pathway"), // rank=178,
																							// count=1321
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/hprdId"), // rank=179,
																							// count=1241
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/meltingPoint"), // rank=180,
																								// count=1159
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/pharmgkbId"), // rank=181,
																								// count=1156
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/contraindicationInsert"), // rank=182,
																											// count=1112
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/halfLife"), // rank=183,
																							// count=1038
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/interactionInsert"), // rank=184,
																										// count=1036
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/ahfsCode"), // rank=185,
																							// count=1015
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/experimentalWaterSolubility"), // rank=186,
																												// count=1009
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/rxlistLink"), // rank=187,
																								// count=998
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/foodInteraction"), // rank=188,
																									// count=996
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/biotransformation"), // rank=189,
																										// count=993
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/dpdDrugIdNumber"), // rank=190,
																									// count=993
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/toxicity"), // rank=191,
																							// count=978
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/absorption"), // rank=192,
																								// count=975
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/keggDrugId"), // rank=193,
																								// count=913
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/msdsFiles"), // rank=194,
																								// count=829
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/l6"), // rank=195,
																		// count=812
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/enzyme"), // rank=196,
																							// count=812
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/proteinBinding"), // rank=197,
																									// count=802
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/patientInformationInsert"), // rank=198,
																											// count=762
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/chebiId"), // rank=199,
																							// count=736
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/fdaLabelFiles"), // rank=200,
																									// count=675
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/tissue"), // rank=201,
																			// count=506
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/l7"), // rank=202,
																		// count=429
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/synthesisReference"), // rank=203,
																										// count=308
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/pkaIsoelectricPoint"), // rank=204,
																										// count=307
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/pdrhealthLink"), // rank=205,
																									// count=280
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/experimentalLogs"), // rank=206,
																									// count=193
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/chemicalStructure"), // rank=207,
																										// count=94
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/experimentalCaco2Permeability"), // rank=208,
																													// count=82
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/genbankId"), // rank=209,
																								// count=71
			new URIImpl(
					"http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/pdbHomologyId"), // rank=210,
																									// count=30
			new URIImpl("http://chem2bio2rdf.org/chembl/resource/l8"), // rank=211,
																		// count=20
	};

	public MyVocabularyDecl() {
	}

	public Iterator<URI> values() {
		return Collections.unmodifiableList(Arrays.asList(uris)).iterator();
	}
}