package benchmark.vocabulary;

import java.util.HashMap;

public class FOAF {
	//The Namespace of this vocabulary as String
	public static final String NS = "http://xmlns.com/foaf/0.1/";

	public static final String PREFIX = "foaf:";
	
	private static HashMap<String, String> uriMap = new HashMap<String, String>();
	
	/*
	 * For prefixed versions
	 */
	public static String prefixed(String string) {
		if(uriMap.containsKey(string)) {
			return uriMap.get(string);
		}
		else {
			String newValue = PREFIX + string;
			uriMap.put(string, newValue);
			return newValue;
		}
	}
	
	//Get the URI of this vocabulary
	public static String getURI() { return NS; }
	
	//Resources
	public static final String Person = NS + "Person";
	
	//Properties
	public static final String homepage = NS + "homepage";
	
	public static final String name = NS + "name";
	
	public static final String mbox_sha1sum = NS + "mbox_sha1sum";
}
