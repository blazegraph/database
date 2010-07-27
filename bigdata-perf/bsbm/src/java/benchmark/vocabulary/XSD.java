package benchmark.vocabulary;

import java.util.HashMap;

public class XSD {
	//The namespace of this vocabulary as String
    public static final String NS = "http://www.w3.org/2001/XMLSchema#";
    
    //Get the URI of this vocabulary
	public static String getURI() { return NS; }
 
	public static final String PREFIX = "xsd:";
	
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
	
	//Resources
	public static final String Integer = NS + "integer";
	
	public static final String Int = NS + "int";
	
	public static final String Float = NS + "float";
	
	public static final String Double = NS + "double";

	public static final String Long = NS + "long";

	public static final String String = NS + "string";

	public static final String Decimal = NS + "decimal";

	public static final String Date = NS + "date";
	
	public static final String DateTime = NS + "dateTime";
}