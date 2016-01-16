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

import java.io.IOException;

import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import com.bigdata.search.ConfigurableAnalyzerFactory.AnalyzerOptions;

/**
 * Unit tests for {@link ConfigurableAnalyzerFactory}.
 * We use the same setup, as defined in {@link #getExtraProperties()}
 * for all the tests. Some of the tests check whether bad combinations
 * of options are detected and reported correctly.
 * Others check that some input, in a particular language is
 * tokenized as expected.
 * @author jeremycarroll
 *
 */
public class TestConfigurableAnalyzerFactory extends AbstractAnalyzerFactoryTest {

	public TestConfigurableAnalyzerFactory() {
	}

	public TestConfigurableAnalyzerFactory(String arg0) {
		super(arg0);
	}

    @Override
	String[] getExtraProperties() {
		String analyzer = ConfigurableAnalyzerFactory.Options.ANALYZER;
		return new String[]{
		FullTextIndex.Options.ANALYZER_FACTORY_CLASS, ConfigurableAnalyzerFactory.class.getName(),
		analyzer+"_."+AnalyzerOptions.LIKE, "x-empty",
		analyzer+"x-empty."+AnalyzerOptions.ANALYZER_CLASS, EmptyAnalyzer.class.getName(),
		analyzer+"x-terms."+AnalyzerOptions.PATTERN, "\\W+",
		analyzer+"x-splits."+AnalyzerOptions.ANALYZER_CLASS, TermCompletionAnalyzer.class.getName(),
		analyzer+"x-splits."+AnalyzerOptions.STOPWORDS, AnalyzerOptions.STOPWORDS_VALUE_NONE,
		analyzer+"x-splits."+AnalyzerOptions.WORD_BOUNDARY, " ",
		analyzer+"x-splits."+AnalyzerOptions.SUB_WORD_BOUNDARY, "(?<!\\p{L}|\\p{N})(?=\\p{L}|\\p{N})|(?<!\\p{Lu})(?=\\p{Lu})|(?<=\\p{N})(?=\\p{L})",
		analyzer+"x-hyphen."+AnalyzerOptions.SUB_WORD_BOUNDARY, "[-.]",
		analyzer+"x-hyphen."+AnalyzerOptions.SOFT_HYPHENS, "-",
		analyzer+"x-hyphen."+AnalyzerOptions.WORD_BOUNDARY, " ",
		analyzer+"x-hyphen."+AnalyzerOptions.ALWAYS_REMOVE_SOFT_HYPHENS, "false",
		analyzer+"x-hyphen2."+AnalyzerOptions.SUB_WORD_BOUNDARY, "[-.]",
		analyzer+"x-hyphen2."+AnalyzerOptions.SOFT_HYPHENS, "-",
		analyzer+"x-hyphen2."+AnalyzerOptions.WORD_BOUNDARY, " ",
		analyzer+"x-hyphen2."+AnalyzerOptions.ALWAYS_REMOVE_SOFT_HYPHENS, "true",
		analyzer+"x-keywords."+AnalyzerOptions.ANALYZER_CLASS, KeywordAnalyzer.class.getName(),
		analyzer+"en-x-de."+AnalyzerOptions.ANALYZER_CLASS, StandardAnalyzer.class.getName(),
		analyzer+"en-x-de."+AnalyzerOptions.STOPWORDS, GermanAnalyzer.class.getName(),
		};
	}
	
	private void badCombo(String errorMessage, String ... props) {
		// Check that some combination of properties on a language create an error
		String myProps[] = new String[props.length+4];
		int i=0;
		for (; i<props.length;i+=2) {
			myProps[i] = ConfigurableAnalyzerFactory.Options.ANALYZER + "x-testme." + props[i];
			myProps[i+1] = props[i+1];
		}
		myProps[i] = ConfigurableAnalyzerFactory.Options.ANALYZER + "_." + AnalyzerOptions.ANALYZER_CLASS;
		myProps[i+1] = EmptyAnalyzer.class.getName();
		myProps[i+2] = FullTextIndex.Options.ANALYZER_FACTORY_CLASS;
		myProps[i+3] = ConfigurableAnalyzerFactory.class.getName();
		try {
		   this.createFullTextIndex("test-in-error"+getName(), myProps).getAnalyzer("en",true);
		}
		catch (RuntimeException e) {
			Throwable t = e;
			while (t.getCause() != null) {
				t = t.getCause();
			}
			assertTrue(t.getMessage(),t.getMessage().contains(errorMessage));
			return;
		}
		fail("No error detected");
	}
	public void testBadLike() {
		badCombo("en-us-x-banana",AnalyzerOptions.LIKE,"en-us-x-banana");
	}
	public void testMissingClass() {
		badCombo("exactly one",AnalyzerOptions.STOPWORDS,AnalyzerOptions.STOPWORDS_VALUE_DEFAULT);
		
	}
	public void testLikeAndClass() {
		badCombo("exactly one",AnalyzerOptions.LIKE,"*", AnalyzerOptions.ANALYZER_CLASS, EmptyAnalyzer.class.getName());
	}
	public void testLikeAndStopwords() {
		badCombo("stopwords",AnalyzerOptions.LIKE,"*", AnalyzerOptions.STOPWORDS,AnalyzerOptions.STOPWORDS_VALUE_DEFAULT);
	}
	public void testCantAlwaysHaveStopWords() {
		badCombo("not supported",
				AnalyzerOptions.ANALYZER_CLASS, EmptyAnalyzer.class.getName(),
				AnalyzerOptions.STOPWORDS,StandardAnalyzer.class.getName()
				);
		
	}
	public void testCantAlwaysHaveDefaultStopWords() {
		badCombo("not supported",
				AnalyzerOptions.ANALYZER_CLASS, EmptyAnalyzer.class.getName(),
				AnalyzerOptions.STOPWORDS,AnalyzerOptions.STOPWORDS_VALUE_DEFAULT
				);
		
	}
	public void testCantFindRussianStopWords() {
		badCombo("find",
				AnalyzerOptions.ANALYZER_CLASS, GermanAnalyzer.class.getName(),
				AnalyzerOptions.STOPWORDS,RussianAnalyzer.class.getName()
				);
		
	}


    public void testEmptyAnalyzer() throws IOException {
    	comparisonTest("en",
    			false,
    			"The fast car arrived slowly.",
    			""
    			);
    	
    }
    
    public void testStopWordSwitch() throws IOException {
    	// en-x-de is an English Analyzer using german stopwords!
    	comparisonTest("en-x-de",
    			true,
    			"The fast car arrived slowly.",
    			"the fast car arrived slowly"
    			);
    	comparisonTest("en-x-de",
    			true,
    			"The fast car die arrived slowly.",
    			"the fast car arrived slowly"
    			);
    	comparisonTest("en-x-de",
    			false,
    			"The fast car die arrived slowly.",
    			"the fast car die arrived slowly"
    			);
    }
    public void testSyapseExample1() throws IOException {
    	comparisonTest("x-splits", 
    			true,
    			"ADENOCARCINOMA OF LUNG, SOMATIC [ERBB2, INS/DUP, NT2322]",
    			"ADENOCARCINOMA OF LUNG, SOMATIC [ERBB2, ERBB2, INS/DUP, DUP, NT2322]"
    			);
    	
    }
    public void testSyapseExample2() throws IOException {
    	comparisonTest("x-splits", 
    			true,
    			"\u2265\u2265\u22653-11.13-11.1",
    			"\u2265\u2265\u22653-11.13-11.1 3-11.13-11.1 11.13-11.1 13-11.1 11.1 1"
    			);
    	
    }
    public void testSyapseExample4() throws IOException {
    	comparisonTest("x-splits", 
    			true,
    			"\u00b1-ACE3.1.1",
    			"\u00b1-ACE3.1.1 ACE3.1.1 1.1 1"
    			);
    	
    }
    public void testSyapseExample3() throws IOException {
    	comparisonTest("x-splits", 
    			true,
    			"2,2,3-trimethylbutane",
    			"2,2,3-trimethylbutane 2,3-trimethylbutane 3-trimethylbutane trimethylbutane"
    			);
    	
    }
    public void testSyapseExample5() throws IOException {
    	comparisonTest("x-splits", 
    			true,
    			"CD8_alpha-low Langerhans cell",
    			"CD8_alpha-low alpha-low low Langerhans cell"
    			);
    	
    }
    public void testSyapseExample6() throws IOException {
    	comparisonTest("x-splits", 
    			true,
    			"6-Monoacetylmorphine:Mass Content:Point in time:Meconium:Quantitative",
    			"6-Monoacetylmorphine:Mass Monoacetylmorphine:Mass Mass Content:Point Point in time:Meconium:Quantitative Meconium:Quantitative Quantitative"
    			);
    	
    }
    public void testSyapseExample7() throws IOException {
    	comparisonTest("x-splits", 
    			true,
    			"N,N-dimethyl",
    			"N,N-dimethyl N-dimethyl dimethyl"
    			);
    	
    }
    public void testSyapseExample8() throws IOException {
    	comparisonTest("x-hyphen", 
    			true,
    			"\u00b1-ACE3.1.1 ab-bc.cd-de",
    			"\u00b1ACE3.1.1 \u00b1-ACE3.1.1 ACE3.1.1 1.1 1 abbc.cdde ab-bc.cd-de bc.cdde bc.cd-de cdde cd-de de"
    			);
    	
    }
    public void testSyapseExample9() throws IOException {
    	comparisonTest("x-hyphen2", 
    			true,
    			"\u00b1-ACE3.1.1 ab-bc.cd-de",
    			"\u00b1ACE3.1.1 ACE3.1.1 1.1 1 abbc.cdde bc.cdde cdde de"
    			);
    	
    }

}
