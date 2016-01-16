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
 * Created on May 7, 2014
 */
package com.bigdata.search;

public class TestDefaultAnalyzerFactory extends AbstractDefaultAnalyzerFactoryTest {

	public TestDefaultAnalyzerFactory() {
	}

	public TestDefaultAnalyzerFactory(String arg0) {
		super(arg0);
	}

	@Override
	String[] getExtraProperties() {
		return new String[0];
	}

	/**
	 * The DefaultAnalyzerFactory has bizarre behavior concerning
	 * language specific settings.
	 * The three letter ISO 639-1 language tags for the languages
	 * for which Lucene has Analyzers use those Analyzers; whereas the two digit ISO
	 * language tags, which are the ones recommended by the IETF and the W3C, 
	 * all use the StandardAnalyzer (English). Also a language tag with a subtag
	 * uses the StandardAnalyzer, even if it is a recognized three letter ISO code.
	 */
	@Override
	boolean isBroken() {
		return true;
	}

	/**
	 * Given legacy concerns, we should preserve the incorrect behavior!
	 */
    public void testIsBroken() {
    	checkConfig(false, "StandardAnalyzer", 
        		"en", "eng", "", null, "ru",
        		"pt", "zh", "por-br", "cs", "dut-za", "nl", "de", "gre-at", "el", "th"); 
    }

}
