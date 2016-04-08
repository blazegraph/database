package com.bigdata.rdf.sail.sparql;

import junit.framework.TestCase;

import org.junit.Test;

/**
 * 
 * Test case for the {@link PrefixDeclProcessor}
 * 
 * @author beebs
 *
 */
public class TestPrefixDeclProcessor extends TestCase {

	@Test
	public void testValidAdditionalPrefixes() {
		final String fileURL = this.getClass().getResource("additional-decls-valid.txt")
				.getFile();
		
		final String decl = "wdref";
		final String declUri = "http://www.wikidata.org/reference/";

		//Since this is static, we need to clean the map;
		PrefixDeclProcessor.defaultDecls.remove(decl);
		
		int cnt = PrefixDeclProcessor.defaultDecls.size();

		assertTrue(!PrefixDeclProcessor.defaultDecls.containsKey(decl));

		System.setProperty(PrefixDeclProcessor.Options.ADDITIONAL_DECLS_FILE, fileURL);
		
		PrefixDeclProcessor.processAdditionalDecls();

		//Validate the decl is added
		assertTrue(PrefixDeclProcessor.defaultDecls.containsKey(decl));

		//The URI is valid
		assertTrue(PrefixDeclProcessor.defaultDecls.get(decl).equals(declUri));
		
		//Validate we added two decls
		assertTrue((cnt + 2) == PrefixDeclProcessor.defaultDecls.size());

	}

	@Test
	public void testInvalidAdditionalPrefixes() {
		final String fileURL = this.getClass().getResource("additional-decls-invalid.txt")
				.getFile();
		
		final String decl = "wdref";
	
		//Since this is static, we need to clean the map;
		PrefixDeclProcessor.defaultDecls.remove(decl);

		int cnt = PrefixDeclProcessor.defaultDecls.size();

		assertTrue(!PrefixDeclProcessor.defaultDecls.containsKey(decl));

		System.setProperty(PrefixDeclProcessor.Options.ADDITIONAL_DECLS_FILE, fileURL);
		
		PrefixDeclProcessor.processAdditionalDecls();

		//Validate no decls were added
		assertTrue((cnt) == PrefixDeclProcessor.defaultDecls.size());
		

	}

	@Test
	public void testFileDoesNotExist() {
		final String fileURL =  "/no/such/file/exists";
		
		final String decl = "wdref";
	
		//Since this is static, we need to clean the map;
		PrefixDeclProcessor.defaultDecls.remove(decl);

		int cnt = PrefixDeclProcessor.defaultDecls.size();

		assertTrue(!PrefixDeclProcessor.defaultDecls.containsKey(decl));

		System.setProperty(PrefixDeclProcessor.Options.ADDITIONAL_DECLS_FILE, fileURL);
		
		PrefixDeclProcessor.processAdditionalDecls();

		assertTrue(!PrefixDeclProcessor.defaultDecls.containsKey(decl));
		
		//Validate no decls were added
		assertTrue((cnt) == PrefixDeclProcessor.defaultDecls.size());

	}
}
