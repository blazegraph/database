package it.unimi.dsi.util;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectRBTreeSet;
import it.unimi.dsi.util.Interval;
import it.unimi.dsi.util.Intervals;
import it.unimi.dsi.util.TernaryIntervalSearchTree;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;


public class TernaryIntervalSearchTreeTest extends TestCase {

	private void assertContains( final TernaryIntervalSearchTree t , final String s, int i ) {
		assertEquals( i, t.getLong( s ) );
		//assertEquals( i, t.get( s.toCharArray() ) );
		assertTrue( t.containsKey( s ) );
		//assertTrue( t.containsKey( s.toCharArray() ) );
	}
	
	private void assertDoesNotContain( final TernaryIntervalSearchTree t , final String s ) {
		assertEquals( -1, t.getLong( s ) );
		//assertEquals( -1, t.get( s.toCharArray() ) );
		assertFalse( t.containsKey( s ) );
		//assertFalse( t.containsKey( s.toCharArray() ) );
	}
	
	public void testSingleString() {
		TernaryIntervalSearchTree t = new TernaryIntervalSearchTree();
		t.add( "test" );
		assertContains( t, "test", 0 );
		assertDoesNotContain( t, "t" );
		assertDoesNotContain( t, "tes" );
		assertEquals( 1, t.size() );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "testx" ) );
		assertEquals( Interval.valueOf( 0 ), t.rangeMap().get( "test" ) );
		assertEquals( Interval.valueOf( 0 ), t.rangeMap().get( "tes" ) );
		assertEquals( Interval.valueOf( 0 ), t.rangeMap().get( "te" ) );
		assertEquals( Interval.valueOf( 0 ), t.rangeMap().get( "t" ) );
		assertEquals( Intervals.EMPTY_INTERVAL, t.getApproximatedInterval( "s" ) );
		assertEquals( Interval.valueOf( 0 ), t.getApproximatedInterval( "t" ) );
		assertEquals( Interval.valueOf( 0 ), t.getApproximatedInterval( "testx" ) );
		assertEquals( "test", t.list().get( 0 ).toString() );
		assertEquals( "test", t.prefixMap().get( Interval.valueOf( 0 ) ).toString() );
	}
	
	public void testForkLeft() {
		TernaryIntervalSearchTree t = new TernaryIntervalSearchTree();
		t.add( "test" );
		t.add( "tast" );
		assertContains( t, "test", 1 );
		assertContains( t, "tast", 0 );
		assertDoesNotContain( t, "t" );
		assertDoesNotContain( t, "tes" );
		assertEquals( 2, t.size() );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "testx" ) );
		assertEquals( Interval.valueOf( 1 ), t.rangeMap().get( "test" ) );
		assertEquals( Interval.valueOf( 1 ), t.rangeMap().get( "tes" ) );
		assertEquals( Interval.valueOf( 1 ), t.rangeMap().get( "te" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.rangeMap().get( "t" ) );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "tx" ) );
		assertEquals( Interval.valueOf( 0, 0 ), t.rangeMap().get( "ta" ) );
		assertEquals( Interval.valueOf( 0, 0 ), t.rangeMap().get( "tas" ) );
		assertEquals( Interval.valueOf( 0, 0 ), t.rangeMap().get( "tast" ) );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "tastx" ) );
		assertEquals( Intervals.EMPTY_INTERVAL, t.getApproximatedInterval( "sz" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.getApproximatedInterval( "t" ) );
		assertEquals( Interval.valueOf( 0 ), t.getApproximatedInterval( "tat" ) );
		assertEquals( Interval.valueOf( 1 ), t.getApproximatedInterval( "tet" ) );
		assertEquals( "tast", t.list().get( 0 ).toString() );
		assertEquals( "test", t.list().get( 1 ).toString() );
		assertEquals( "tast", t.prefixMap().get( Interval.valueOf( 0 ) ).toString() );
		assertEquals( "test", t.prefixMap().get( Interval.valueOf( 1 ) ).toString() );
		assertEquals( "t", t.prefixMap().get( Interval.valueOf( 0, 1 ) ).toString() );
	}

	public void testForkRight() {
		TernaryIntervalSearchTree t = new TernaryIntervalSearchTree();
		t.add( "test" );
		t.add( "tust" );
		assertContains( t, "test", 0 );
		assertContains( t, "tust", 1 );
		assertDoesNotContain( t, "t" );
		assertDoesNotContain( t, "tes" );
		assertEquals( 2, t.size() );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "tustx" ) );
		assertEquals( Interval.valueOf( 1 ), t.rangeMap().get( "tust" ) );
		assertEquals( Interval.valueOf( 1 ), t.rangeMap().get( "tus" ) );
		assertEquals( Interval.valueOf( 1 ), t.rangeMap().get( "tu" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.rangeMap().get( "t" ) );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "tx" ) );
		assertEquals( Interval.valueOf( 0, 0 ), t.rangeMap().get( "te" ) );
		assertEquals( Interval.valueOf( 0, 0 ), t.rangeMap().get( "tes" ) );
		assertEquals( Interval.valueOf( 0, 0 ), t.rangeMap().get( "test" ) );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "testx" ) );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "testx" ) );
		assertEquals( Intervals.EMPTY_INTERVAL, t.getApproximatedInterval( "ta" ) );
		assertEquals( Intervals.EMPTY_INTERVAL, t.getApproximatedInterval( "s" ) );
		assertEquals( Interval.valueOf( 0 ), t.getApproximatedInterval( "tf" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.getApproximatedInterval( "t" ) );
		assertEquals( Interval.valueOf( 0 ), t.getApproximatedInterval( "tet" ) );
		assertEquals( Interval.valueOf( 1 ), t.getApproximatedInterval( "tut" ) );
		assertEquals( "test", t.list().get( 0 ).toString() );
		assertEquals( "tust", t.list().get( 1 ).toString() );
		assertEquals( "test", t.prefixMap().get( Interval.valueOf( 0 ) ).toString() );
		assertEquals( "tust", t.prefixMap().get( Interval.valueOf( 1 ) ).toString() );
		assertEquals( "t", t.prefixMap().get( Interval.valueOf( 0, 1 ) ).toString() );
	}

	public void testForkMiddle() {
		TernaryIntervalSearchTree t = new TernaryIntervalSearchTree();
		t.add( "te" );
		t.add( "test" );
		assertContains( t, "test", 1 );
		assertContains( t, "te", 0 );
		assertDoesNotContain( t, "t" );
		assertDoesNotContain( t, "tes" );
		assertEquals( 2, t.size() );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "tx" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.rangeMap().get( "te" ) );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "tex" ) );
		assertEquals( Interval.valueOf( 1 ), t.rangeMap().get( "tes" ) );
		assertEquals( Interval.valueOf( 1 ), t.rangeMap().get( "test" ) );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "testx" ) );
		assertEquals( Intervals.EMPTY_INTERVAL, t.getApproximatedInterval( "s" ) );
		assertEquals( Interval.valueOf( 1 ), t.getApproximatedInterval( "tet" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.getApproximatedInterval( "t" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.getApproximatedInterval( "te" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.getApproximatedInterval( "tes" ) );
		assertEquals( Interval.valueOf( 1 ), t.getApproximatedInterval( "tt" ) );
		assertEquals( "te", t.list().get( 0 ).toString() );
		assertEquals( "test", t.list().get( 1 ).toString() );
		assertEquals( "te", t.prefixMap().get( Interval.valueOf( 0 ) ).toString() );
		assertEquals( "test", t.prefixMap().get( Interval.valueOf( 1 ) ).toString() );
		assertEquals( "te", t.prefixMap().get( Interval.valueOf( 0, 1 ) ).toString() );
	}

	public void testSplit() {
		TernaryIntervalSearchTree t = new TernaryIntervalSearchTree();
		t.add( "test" );
		t.add( "te" );
		assertContains( t, "test", 1 );
		assertContains( t, "te", 0 );
		assertDoesNotContain( t, "t" );
		assertDoesNotContain( t, "tes" );
		assertEquals( 2, t.size() );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "tx" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.rangeMap().get( "te" ) );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "tex" ) );
		assertEquals( Interval.valueOf( 1 ), t.rangeMap().get( "tes" ) );
		assertEquals( Interval.valueOf( 1 ), t.rangeMap().get( "test" ) );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "testx" ) );
		assertEquals( Intervals.EMPTY_INTERVAL, t.getApproximatedInterval( "s" ) );
		assertEquals( Interval.valueOf( 1 ), t.getApproximatedInterval( "tet" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.getApproximatedInterval( "t" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.getApproximatedInterval( "te" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.getApproximatedInterval( "tes" ) );
		assertEquals( Interval.valueOf( 1 ), t.getApproximatedInterval( "tt" ) );
		assertEquals( "te", t.list().get( 0 ).toString() );
		assertEquals( "test", t.list().get( 1 ).toString() );
		assertEquals( "te", t.prefixMap().get( Interval.valueOf( 0 ) ).toString() );
		assertEquals( "test", t.prefixMap().get( Interval.valueOf( 1 ) ).toString() );
		assertEquals( "te", t.prefixMap().get( Interval.valueOf( 0, 1 ) ).toString() );
	}

	public void testForkLeftLate() {
		TernaryIntervalSearchTree t = new TernaryIntervalSearchTree();
		t.add( "test" );
		t.add( "tess" );
		assertContains( t, "test", 1 );
		assertContains( t, "tess", 0 );
		assertDoesNotContain( t, "t" );
		assertDoesNotContain( t, "tes" );
		assertEquals( 2, t.size() );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "testx" ) );
		assertEquals( Interval.valueOf( 1 ), t.rangeMap().get( "test" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.rangeMap().get( "tes" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.rangeMap().get( "te" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.rangeMap().get( "t" ) );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "tx" ) );
		assertEquals( Interval.valueOf( 0, 0 ), t.rangeMap().get( "tess" ) );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "tessx" ) );
		assertEquals( Intervals.EMPTY_INTERVAL, t.getApproximatedInterval( "sz" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.getApproximatedInterval( "t" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.getApproximatedInterval( "te" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.getApproximatedInterval( "tes" ) );
		assertEquals( Interval.valueOf( 0 ), t.getApproximatedInterval( "tessz" ) );
		assertEquals( Interval.valueOf( 1 ), t.getApproximatedInterval( "testz" ) );
		assertEquals( Interval.valueOf( 1 ), t.getApproximatedInterval( "tet" ) );
		assertEquals( "tess", t.list().get( 0 ).toString() );
		assertEquals( "test", t.list().get( 1 ).toString() );
		assertEquals( "tess", t.prefixMap().get( Interval.valueOf( 0 ) ).toString() );
		assertEquals( "test", t.prefixMap().get( Interval.valueOf( 1 ) ).toString() );
		assertEquals( "tes", t.prefixMap().get( Interval.valueOf( 0, 1 ) ).toString() );
	}

	public void testForkRightLate() {
		TernaryIntervalSearchTree t = new TernaryIntervalSearchTree();
		t.add( "test" );
		t.add( "tesv" );
		assertContains( t, "test", 0 );
		assertContains( t, "tesv", 1 );
		assertDoesNotContain( t, "t" );
		assertDoesNotContain( t, "tes" );
		assertEquals( 2, t.size() );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "tesvx" ) );
		assertEquals( Interval.valueOf( 1 ), t.rangeMap().get( "tesv" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.rangeMap().get( "tes" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.rangeMap().get( "te" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.rangeMap().get( "t" ) );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "tx" ) );
		assertEquals( Interval.valueOf( 0, 0 ), t.rangeMap().get( "test" ) );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "testx" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.getApproximatedInterval( "t" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.getApproximatedInterval( "te" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.getApproximatedInterval( "tes" ) );
		assertEquals( Interval.valueOf( 0 ), t.getApproximatedInterval( "tesu" ) );
		assertEquals( Interval.valueOf( 0 ), t.getApproximatedInterval( "testvz" ) );
		assertEquals( Interval.valueOf( 1 ), t.getApproximatedInterval( "tet" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.getApproximatedInterval( "tes" ) );
		assertEquals( "test", t.list().get( 0 ).toString() );
		assertEquals( "tesv", t.list().get( 1 ).toString() );
		assertEquals( "test", t.prefixMap().get( Interval.valueOf( 0 ) ).toString() );
		assertEquals( "tesv", t.prefixMap().get( Interval.valueOf( 1 ) ).toString() );
		assertEquals( "tes", t.prefixMap().get( Interval.valueOf( 0, 1 ) ).toString() );
	}

	public void testForkMiddleLate() {
		TernaryIntervalSearchTree t = new TernaryIntervalSearchTree();
		t.add( "tes" );
		t.add( "test" );
		assertContains( t, "test", 1 );
		assertContains( t, "tes", 0 );
		assertDoesNotContain( t, "t" );
		assertDoesNotContain( t, "te" );
		assertEquals( 2, t.size() );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "tx" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.rangeMap().get( "te" ) );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "tex" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.rangeMap().get( "tes" ) );
		assertEquals( Interval.valueOf( 1 ), t.rangeMap().get( "test" ) );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "testx" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.getApproximatedInterval( "t" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.getApproximatedInterval( "te" ) );
		assertEquals( Interval.valueOf( 0, 1 ), t.getApproximatedInterval( "tes" ) );
		assertEquals( Interval.valueOf( 0 ), t.getApproximatedInterval( "tess" ) );
		assertEquals( Interval.valueOf( 1 ), t.getApproximatedInterval( "testvz" ) );
		assertEquals( Interval.valueOf( 1 ), t.getApproximatedInterval( "tet" ) );
		assertEquals( "tes", t.list().get( 0 ).toString() );
		assertEquals( "test", t.list().get( 1 ).toString() );
		assertEquals( "tes", t.prefixMap().get( Interval.valueOf( 0 ) ).toString() );
		assertEquals( "test", t.prefixMap().get( Interval.valueOf( 1 ) ).toString() );
		assertEquals( "tes", t.prefixMap().get( Interval.valueOf( 0, 1 ) ).toString() );
	}

	public void testJustMarkNode() {
		TernaryIntervalSearchTree t = new TernaryIntervalSearchTree();
		t.add( "test" );
		t.add( "tast" );
		t.add( "te" );
		assertContains( t, "test", 2 );
		assertContains( t, "tast", 0 );
		assertContains( t, "te", 1 );
		assertEquals( 3, t.size() );
		assertDoesNotContain( t, "t" );
		assertDoesNotContain( t, "tes" );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "testx" ) );
		assertEquals( Interval.valueOf( 2 ), t.rangeMap().get( "test" ) );
		assertEquals( Interval.valueOf( 2 ), t.rangeMap().get( "tes" ) );
		assertEquals( Interval.valueOf( 1, 2 ), t.rangeMap().get( "te" ) );
		assertEquals( Interval.valueOf( 0, 2 ), t.rangeMap().get( "t" ) );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "tx" ) );
		assertEquals( Interval.valueOf( 0, 0 ), t.rangeMap().get( "tas" ) );
		assertEquals( Interval.valueOf( 0, 0 ), t.rangeMap().get( "tast" ) );
		assertEquals( Intervals.EMPTY_INTERVAL, t.rangeMap().get( "tastx" ) );
		assertEquals( Interval.valueOf( 0, 2 ), t.getApproximatedInterval( "t" ) );
		assertEquals( Interval.valueOf( 0, 0 ), t.getApproximatedInterval( "ta" ) );
		assertEquals( Interval.valueOf( 1, 2 ), t.getApproximatedInterval( "te" ) );
		assertEquals( Interval.valueOf( 1, 2 ), t.getApproximatedInterval( "tes" ) );
		assertEquals( Interval.valueOf( 2 ), t.getApproximatedInterval( "testvz" ) );
		assertEquals( Interval.valueOf( 2 ), t.getApproximatedInterval( "tet" ) );
		assertEquals( "tast", t.list().get( 0 ).toString() );
		assertEquals( "te", t.list().get( 1 ).toString() );
		assertEquals( "tast", t.prefixMap().get( Interval.valueOf( 0 ) ).toString() );
		assertEquals( "te", t.prefixMap().get( Interval.valueOf( 1 ) ).toString() );
		assertEquals( "test", t.prefixMap().get( Interval.valueOf( 2 ) ).toString() );
		assertEquals( "t", t.prefixMap().get( Interval.valueOf( 0, 1 ) ).toString() );
		assertEquals( "te", t.prefixMap().get( Interval.valueOf( 1, 2 ) ).toString() );
		assertEquals( "t", t.prefixMap().get( Interval.valueOf( 0, 2 ) ).toString() );
	}
	
	public void testTwoRightForks() {
		TernaryIntervalSearchTree t = new TernaryIntervalSearchTree();
		t.add( "0" );
		t.add( "iscrivit" );
		t.add( "vai" );
		assertEquals( Interval.valueOf( 0, 1 ), t.getApproximatedInterval( "i" ) );
		assertEquals( "0", t.prefixMap().get( Interval.valueOf( 0 ) ).toString() );
		assertEquals( "iscrivit", t.prefixMap().get( Interval.valueOf( 1 ) ).toString() );
		assertEquals( "vai", t.prefixMap().get( Interval.valueOf( 2 ) ).toString() );
		assertEquals( "", t.prefixMap().get( Interval.valueOf( 0, 1 ) ).toString() );
		assertEquals( "", t.prefixMap().get( Interval.valueOf( 1, 2 ) ).toString() );
		assertEquals( "", t.prefixMap().get( Interval.valueOf( 0, 2 ) ).toString() );
	}
		
	public void testLargeSet() {
		long seed = System.currentTimeMillis();
		System.err.println( seed );
		List<String> c = new ObjectArrayList<String>( WORDS );
		Collections.shuffle( c );
		TernaryIntervalSearchTree t = new TernaryIntervalSearchTree( c );
		
		for( int i = 0; i < WORDS.length; i++ ) assertTrue( WORDS[ i ], t.containsKey( WORDS[ i ] ) );
		for( int i = 0; i < WORDS.length; i++ ) assertEquals( WORDS[ i ], t.list().get( i ).toString() );

		for( int i = 0; i < WORDS.length; i++ ) 
			for( int j = 0; j < WORDS[ i ].length(); j++ ) {
				String s = WORDS[ i ].substring( 0, j + 1 );
				int k, left, right;
				for( k = 0; k < WORDS.length; k++ ) if ( WORDS[ k ].startsWith( s ) ) break;
				left = k;
				for( ; k < WORDS.length; k++ ) if ( ! WORDS[ k ].startsWith( s ) ) break;
				right = k - 1;
				
				assertEquals( s, left <= right ? Interval.valueOf( left, right ) : Intervals.EMPTY_INTERVAL, t.rangeMap().get( s ) );

				s = s + " ";
				for( k = 0; k < WORDS.length; k++ ) if ( s.compareTo( WORDS[ k ] ) < 0 ) break;
				
				assertEquals( s, k > 0 ? Interval.valueOf( k - 1 ) : Intervals.EMPTY_INTERVAL, t.getApproximatedInterval( s ) );
				
				s = s.substring( 0, s.length() - 1 ) + "~";

				for( k = 0; k < WORDS.length; k++ ) if ( s.compareTo( WORDS[ k ] ) < 0 ) break;
				
				assertEquals( s, Interval.valueOf( k - 1 ), t.getApproximatedInterval( s ) );
			}
		
		Collection<String> p = new ObjectRBTreeSet<String>();
		for( int i = 0; i < WORDS.length; i++ ) 
			for( int j = 0; j < WORDS[ i ].length(); j++ )
				p.add( WORDS[ i ].substring( 0, j + 1 ) );
		t = new TernaryIntervalSearchTree( p );
		
		for( Iterator<String> i = p.iterator(); i.hasNext(); ) {
			String s = i.next();
			assertTrue( s, t.containsKey( s ) );
		}

		int j = 0;
		for( Iterator<String> i = p.iterator(); i.hasNext(); ) {
			String s = i.next();
			assertEquals( s, t.list().get( j++ ).toString() );
		}

	}
	
	public final static String[] WORDS = { "0", "00", "01", "02", "0200", "03", "09", "1", "10", "100", "11", "12", "13", "14", "15", "15mb", "18", "19", "1999", "2", "20", "2000", "2003", "2004", 
			"2430", "27", "28", "3", "30", "33", "3d", "4", "5", "50", "6", "61", "7", "7027", "8", "9", "96", "a", "abiti", "accanto", "accesso", "accordo", "ad", "addio", "adsl", "aerei", "aereo",
			"affari", "affitto", "aforismi", "ai", "aiuto", "al", "alan", "alcune", "alcuni", "all", "alla", "alta", "altra", "altre", "altri", "altro", "ambo", "american", "amici", "amico", "amore",
			"ampia", "anche", "anima", "anni", "anno", "annunci", "annuncio", "anticoncezionali", "api", "appelli", "appuntamento", "aquario", "argomento", "arianna", "arrivi", "arte", "aspettano",
			"assicurazioni", "aste", "auto", "autore", "autoworld", "avermi", "avrai", "avuto", "avventura", "avvocati", "avvocato", "aziendale", "aziende", "azione", "azioni", "b", "b2b", "baci",
			"banche", "barra", "barzelletta", "barzellette", "basket", "basta", "battute", "battutine", "bella", "belle", "bello", "bellucci", "ben", "benessere", "benvenuti", "biasimo", "bilancia",
			"bisogno", "bisturi", "blu", "bob", "borsa", "borse", "borsellino", "brevi", "buffe", "buona", "business", "c", "cabaret", "calcio", "calcola", "calendari", "calorie", "camera",
			"campeggio", "canale", "canali", "canone", "canzone", "capacità", "capisce", "capodanno", "carabinieri", "carta", "cartoline", "cartoni", "casa", "casaclick", "case", "categorie",
			"cattiverie", "ce", "cellulare", "center", "centesimi", "cerca", "cercatrova", "cerchi", "champions", "chat", "che", "chi", "chiedi", "chilo", "chiuso", "cinema", "citta", "città",
			"ciunga", "classe", "classifica", "classifiche", "clicca", "cliccati", "clienti", "collegati", "collegato", "colleghi", "com", "come", "comico", "commedia", "community", "compra",
			"computer", "computers", "con", "conosci", "console", "consulenze", "consulti", "contattaci", "contenuti", "continua", "copyright", "corpo", "correzioni", "cortometrag", "cosa", "cose",
			"crea", "credito", "cronache", "cucina", "cui", "cultura", "cupido", "cv", "d", "da", "dai", "dal", "dalla", "date", "decisamente", "dei", "del", "dell", "della", "delle", "dello",
			"desideri", "di", "dieta", "dietro", "difficoltà", "digiland", "digitale", "directory", "diritti", "disclaimer", "discorso", "discussioni", "disposizione", "diteci", "diversi",
			"divertente", "divertenti", "divertimento", "divorzio", "documentario", "dom", "domanda", "domini", "donne", "dopo", "download", "drammatico", "dreamcast", "dubbi", "due", "durare", "e",
			"easysms", "ebay", "ecocultura", "ecommerce", "economia", "edicola", "editoria", "editoriale", "editoriali", "elenco", "elisa", "erotico", "esclusivi", "etaslab", "eterni", "eterno",
			"euro", "evoluzione", "excite", "express", "extra", "fa", "facile", "fai", "fantascienza", "fare", "fasi", "fatto", "febbraio", "ferro", "feste", "festival", "figli", "figlia", "figlio",
			"filastrocche", "files", "film", "finalmente", "finanza", "finora", "fiorellini", "fissa", "fitness", "flat", "folli", "fondi", "fortuna", "forum", "foto", "fotografia", "freddure",
			"free", "fumetti", "fun", "galleria", "gallerie", "gallery", "gamecube", "games", "garanzie", "gemella", "gemelli", "generale", "genere", "generi", "gente", "gi", "giallo", "gio",
			"gioca", "giochi", "gioco", "giornalistiche", "giorno", "giurisdizioni", "giusta", "gli", "google", "gossip", "gratis", "gratuitamente", "gratuiti", "grottesco", "guadagna", "guadagni",
			"guardare", "guest", "guida", "ha", "hai", "ho", "home", "hope", "horror", "hosting", "hotel", "humor", "i", "idee", "il", "immagini", "improbabile", "in", "incinta", "incontri",
			"indice", "indirizzi", "infernali", "informazioni", "iniziativa", "inseriti", "interagisci", "internet", "interromperti", "intrattenimento", "invia", "inwind", "iol", "irriverenti",
			"iscrivermi", "iscriviti", "it", "italia", "italiani", "italiaonline", "jumpy", "l", "la", "lastminute", "laurea", "lavagna", "lavatrice", "lavoro", "le", "legge", "leggendo", "leggerlo",
			"leggi", "libero", "libri", "life", "line", "live", "lo", "località", "loghi", "loro", "lotto", "lui", "lun", "lunedì", "lycos", "macchine", "mai", "mail", "mailbox", "mappe", "mar",
			"marito", "mars", "matrimonio", "matte", "matti", "maturando", "medicoonline", "meglio", "memoria", "menù", "mer", "mercatino", "mercedes", "messo", "meteo", "mette", "mib", "mibtel",
			"miglior", "migliori", "milano", "mio", "misteri", "miti", "mms", "moda", "mode", "modem", "modo", "moglie", "molto", "mondo", "mostralfonso", "motori", "moviola", "mp3", "multiservizi",
			"musica", "musical", "mutui", "mutuo", "n64", "napoli", "nascosti", "nascosto", "nasdaq", "natura", "naviga", "nazionale", "ne", "negativo", "nel", "nella", "nelle", "neve", "news",
			"newsgroup", "newsletter", "nikkei", "non", "notebook", "notizie", "novita", "nuova", "nuovi", "o", "offerte", "offre", "ogni", "on", "online", "ora", "ordine", "ore", "oroscopo", "orsa",
			"oscar", "ottima", "ottimizza", "ovviamente", "p", "padre", "page", "palco", "panoramica", "parecchi", "parenti", "parla", "partite", "partner", "pass", "passioni", "pazze", "pc", "pena",
			"per", "perdere", "personal", "personalizzati", "persone", "piace", "pianeta", "piangono", "pianificare", "piatti", "picchio", "pillole", "pippo", "pippoland", "piu", "più",
			"playstation", "poesie", "polemica", "policy", "poliziesco", "pop", "porno", "porta", "positivo", "possibile", "posta", "poveri", "povertà", "praticità", "preferenza", "preferiti",
			"premi", "premio", "premium", "prese", "presentaci", "prestiti", "prestito", "previsioni", "prezzi", "primo", "privacy", "problema", "prodotti", "professionale", "progetto", "promozioni",
			"proposte", "prostituta", "prova", "ps2", "pubblicheremo", "pubblicita", "punti", "puoi", "qualche", "qualcosa", "quelle", "quello", "questa", "queste", "questi", "questo", "qui",
			"raccolte", "raccontato", "radio", "rapido", "rassegna", "realizzazione", "recensioni", "recensiti", "relazione", "religione", "reserved", "responsabile", "ricchezza", "ricchi",
			"ricerca", "ricerche", "ricette", "ricevi", "richiedi", "ridono", "rights", "rime", "risate", "riservati", "riso", "risolti", "risparmi", "rno", "roma", "rosso", "rotta", "rubriche", "s",
			"sab", "sacco", "sali", "salute", "san", "saretefamosi", "sarà", "scambia", "scarica", "scaricare", "scegli", "scelta", "scelti", "scienza", "scienze", "sconosciuto", "scontati",
			"sconti", "scopri", "scoprilo", "scrivi", "scrivici", "se", "secondo", "segnala", "segreti", "sei", "sembrano", "sempre", "seno", "sentimentale", "sera", "serpenti", "serve", "servizi",
			"sessi", "settimana", "sfido", "sfortuna", "sfortunata", "shirt", "shop", "shopping", "si", "sia", "siamo", "signora", "silicone", "simpatiche", "siti", "sito", "smeraldo", "sms",
			"snake", "software", "sognato", "soli", "solo", "soluzione", "soluzioni", "son", "sondaggio", "sono", "sotto", "special", "speciale", "speciali", "spediscila", "spettacolari", "spicy",
			"sport", "srl", "sta", "stampa", "stanno", "stanza", "stasera", "stella", "stellare", "stelle", "storia", "storico", "storie", "stranafoto", "strane", "strani", "studia", "stupidario",
			"stupisci", "su", "sua", "subito", "successo", "sugli", "sul", "sulla", "suo", "suonerie", "suoni", "super", "supertop", "sviluppatori", "t", "tante", "tantum", "tarocco", "tassi", "te",
			"tecnologia", "teknosurf", "telefonia", "telefonino", "televisione", "tempo", "territorio", "testa", "thriller", "ti", "tipiche", "toccano", "top", "tra", "traduzioni", "tranquillo",
			"trasloca", "trasloco", "tre", "tribù", "troppo", "trova", "trovi", "trucchi", "tu", "tua", "tue", "tuo", "tuoi", "tutta", "tutte", "tutti", "tutto", "tuttocasa", "tv", "uccelli", "uffa",
			"ultim", "ultimi", "umoristico", "un", "una", "unico", "uomini", "uomo", "user", "utili", "vacanze", "vale", "valentino", "varie", "vasectomia", "veloce", "velocemente", "ven", "vendita",
			"vero", "vetrine", "via", "viaggi", "viaggiare", "viaggio", "video", "videogiochi", "vignette", "vinci", "virgilio", "virtuali", "visita", "vista", "vivere", "voglio", "voi", "volta",
			"vostre", "voto", "vuoi", "vuole", "wap", "web", "webmaster", "western", "www", "xbox", "yahoo", "zone" };

}
