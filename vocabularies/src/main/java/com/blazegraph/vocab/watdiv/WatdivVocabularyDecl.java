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
package com.blazegraph.vocab.watdiv;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import com.bigdata.rdf.vocab.VocabularyDecl;

public class WatdivVocabularyDecl implements VocabularyDecl {

	static private final URI[] uris = new URI[] {

			// Inline URIs
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/AgeGroup"),
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/City"),
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/Country"),
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/Gender"),
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/Genre"),
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/Language"),
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/Offer"),
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/Product"),
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory"),
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/Purchase"),
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/Retailer"),
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/Review"),
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/Role"),
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre"),
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/Topic"),
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/User"),
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/Website"),

			// frequencies of predicates in dataset
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/friendOf"), // rank=0,
																			// count=4491142
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/follows"), // rank=1,
																		// count=3289307
			new URIImpl("http://purl.org/goodrelations/price"), // rank=2,
																// count=240000
			new URIImpl("http://schema.org/eligibleRegion"), // rank=3,
																// count=183550
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/subscribes"), // rank=4,
																			// count=152275
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/purchaseFor"), // rank=5,
																			// count=150000
			new URIImpl("http://purl.org/stuff/rev#rating"), // rank=6,
																// count=150000
			new URIImpl("http://purl.org/stuff/rev#reviewer"), // rank=7,
																// count=150000
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/makesPurchase"), // rank=8,
																				// count=149998
			new URIImpl("http://purl.org/stuff/rev#hasReview"), // rank=9,
																// count=149634
			new URIImpl("http://ogp.me/ns#tag"), // rank=10, count=147271
			new URIImpl("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), // rank=11,
																			// count=136215
			new URIImpl("http://purl.org/goodrelations/offers"), // rank=12,
																	// count=119316
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/likes"), // rank=13,
																		// count=112401
			new URIImpl("http://purl.org/stuff/rev#text"), // rank=14,
															// count=104994
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/userId"), // rank=15,
																		// count=100000
			new URIImpl("http://schema.org/email"), // rank=16, count=91004
			new URIImpl("http://purl.org/goodrelations/includes"), // rank=17,
																	// count=90000
			new URIImpl("http://purl.org/goodrelations/serialNumber"), // rank=18,
																		// count=90000
			new URIImpl("http://schema.org/eligibleQuantity"), // rank=19,
																// count=90000
			new URIImpl("http://xmlns.com/foaf/familyName"), // rank=20,
																// count=69970
			new URIImpl("http://xmlns.com/foaf/givenName"), // rank=21,
															// count=69970
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/gender"), // rank=22,
																		// count=59784
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre"), // rank=23,
																			// count=58787
			new URIImpl("http://xmlns.com/foaf/age"), // rank=24, count=50095
			new URIImpl("http://purl.org/stuff/rev#title"), // rank=25,
															// count=44830
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/purchaseDate"), // rank=26,
																				// count=44721
			new URIImpl("http://purl.org/dc/terms/Location"), // rank=27,
																// count=40297
			new URIImpl("http://purl.org/goodrelations/validThrough"), // rank=28,
																		// count=36346
			new URIImpl("http://purl.org/goodrelations/validFrom"), // rank=29,
																	// count=36250
			new URIImpl("http://ogp.me/ns#title"), // rank=30, count=25000
			new URIImpl("http://schema.org/birthDate"), // rank=31, count=20128
			new URIImpl("http://schema.org/nationality"), // rank=32,
															// count=19924
			new URIImpl("http://schema.org/priceValidUntil"), // rank=33,
																// count=17899
			new URIImpl("http://schema.org/actor"), // rank=34, count=15991
			new URIImpl("http://schema.org/description"), // rank=35,
															// count=14960
			new URIImpl("http://xmlns.com/foaf/homepage"), // rank=36,
															// count=11204
			new URIImpl("http://purl.org/stuff/rev#totalVotes"), // rank=37,
																	// count=7554
			new URIImpl("http://schema.org/contentRating"), // rank=38,
															// count=7530
			new URIImpl("http://schema.org/text"), // rank=39, count=7476
			new URIImpl("http://schema.org/keywords"), // rank=40, count=7410
			new URIImpl("http://schema.org/language"), // rank=41, count=6251
			new URIImpl("http://schema.org/telephone"), // rank=42, count=5843
			new URIImpl("http://schema.org/jobTitle"), // rank=43, count=5008
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/hits"), // rank=44,
																		// count=5000
			new URIImpl("http://schema.org/url"), // rank=45, count=5000
			new URIImpl("http://schema.org/author"), // rank=46, count=3975
			new URIImpl("http://schema.org/caption"), // rank=47, count=2501
			new URIImpl("http://schema.org/contentSize"), // rank=48, count=2438
			new URIImpl("http://schema.org/isbn"), // rank=49, count=1659
			new URIImpl("http://schema.org/editor"), // rank=50, count=1578
			new URIImpl("http://schema.org/publisher"), // rank=51, count=1350
			new URIImpl("http://purl.org/ontology/mo/artist"), // rank=52,
																// count=1335
			new URIImpl("http://schema.org/director"), // rank=53, count=1312
			new URIImpl("http://schema.org/employee"), // rank=54, count=1202
			new URIImpl("http://purl.org/goodrelations/description"), // rank=55,
																		// count=1200
			new URIImpl("http://purl.org/goodrelations/name"), // rank=56,
																// count=1200
			new URIImpl("http://schema.org/expires"), // rank=57, count=1184
			new URIImpl("http://schema.org/datePublished"), // rank=58,
															// count=1157
			new URIImpl("http://schema.org/openingHours"), // rank=59, count=962
			new URIImpl("http://schema.org/contactPoint"), // rank=60, count=953
			new URIImpl("http://purl.org/ontology/mo/record_number"), // rank=61,
																		// count=850
			new URIImpl("http://schema.org/bookEdition"), // rank=62, count=847
			new URIImpl("http://purl.org/ontology/mo/release"), // rank=63,
																// count=842
			new URIImpl("http://purl.org/ontology/mo/producer"), // rank=64,
																	// count=820
			new URIImpl("http://schema.org/producer"), // rank=65, count=790
			new URIImpl("http://schema.org/paymentAccepted"), // rank=66,
																// count=703
			new URIImpl("http://purl.org/ontology/mo/movement"), // rank=67,
																	// count=651
			new URIImpl("http://purl.org/ontology/mo/opus"), // rank=68,
																// count=651
			new URIImpl("http://schema.org/award"), // rank=69, count=519
			new URIImpl("http://schema.org/duration"), // rank=70, count=506
			new URIImpl("http://schema.org/aggregateRating"), // rank=71,
																// count=497
			new URIImpl("http://purl.org/ontology/mo/performer"), // rank=72,
																	// count=412
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/composer"), // rank=73,
																			// count=412
			new URIImpl("http://schema.org/numberOfPages"), // rank=74,
															// count=411
			new URIImpl("http://purl.org/ontology/mo/performed_in"), // rank=75,
																		// count=402
			new URIImpl("http://purl.org/ontology/mo/conductor"), // rank=76,
																	// count=401
			new URIImpl("http://schema.org/wordCount"), // rank=77, count=344
			new URIImpl("http://schema.org/printColumn"), // rank=78, count=344
			new URIImpl("http://schema.org/printEdition"), // rank=79, count=344
			new URIImpl("http://schema.org/printPage"), // rank=80, count=325
			new URIImpl("http://schema.org/printSection"), // rank=81, count=325
			new URIImpl("http://schema.org/trailer"), // rank=82, count=257
			new URIImpl("http://www.geonames.org/ontology#parentCountry"), // rank=83,
																			// count=240
			new URIImpl("http://schema.org/faxNumber"), // rank=84, count=115
			new URIImpl("http://schema.org/legalName"), // rank=85, count=108
			// frequencies of types in dataset
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/Role0"), // rank=0,
																		// count=58629
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/Role1"), // rank=1,
																		// count=31173
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/Role2"), // rank=2,
																		// count=21268
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory14"), // rank=3,
																					// count=2586
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory9"), // rank=4,
																					// count=1719
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory12"), // rank=5,
																					// count=1711
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory5"), // rank=6,
																					// count=1687
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory11"), // rank=7,
																					// count=1686
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory10"), // rank=8,
																					// count=1673
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory3"), // rank=9,
																					// count=1659
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory1"), // rank=10,
																					// count=1658
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory8"), // rank=11,
																					// count=1649
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory6"), // rank=12,
																					// count=1645
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory4"), // rank=13,
																					// count=1637
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory2"), // rank=14,
																					// count=1634
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory13"), // rank=15,
																					// count=1630
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory7"), // rank=16,
																					// count=1619
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory0"), // rank=17,
																					// count=807
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/Genre20"), // rank=18,
																		// count=12
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/Genre19"), // rank=19,
																		// count=11
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/Genre15"), // rank=20,
																		// count=10
			new URIImpl("http://db.uwaterloo.ca/~galuc/wsdbm/Genre6"), // rank=21,
																		// count=10
	};

	public WatdivVocabularyDecl() {
	}

	public Iterator<URI> values() {
		return Collections.unmodifiableList(Arrays.asList(uris)).iterator();
	}
}
