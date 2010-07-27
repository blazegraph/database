package benchmark.vocabulary;

import java.util.*;

public class BSBM {
	//The Namespace of this vocabulary as String
	public static String NS = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/";
	
	public static String PREFIX = "bsbm:";
	
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
	
	//Namespace of the instances for single source RDF stores	
	public static  String INST_NS = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/";
	
	public static  String INST_PREFIX = "bsbm-inst:";
	
	//Get the URI of this vocabulary
	public static String getURI() { return NS; }
		
	//Resource type: Offer
	public static  String Offer = (NS+ "Offer");
	
	//Resource type: Vendor
	public static  String Vendor = (NS + "Vendor");
	
	//Resource type: Producer
	public static  String Producer = (NS + "Producer");
	
	//Resource type: ProductFeature
	public static  String ProductFeature = (NS + "ProductFeature");
	
	//Resource type: ProductCategory
	public static  String ProductCategory = (NS + "ProductCategory");
	
	//Resource type: ProductType
	public static  String ProductType = (NS + "ProductType");

	//Resource type: Product
	public static  String Product = (NS + "Product");
	
	//Property: productFeature
	public static  String productFeature = (NS + "productFeature");
	
	//Property: productCategory
	public static  String productCategory = (NS + "productCategory");
	
	//Property: producer
	public static  String producer = (NS + "producer");
	
	//Property: productStringTextual1
	public static  String productPropertyTextual1 = (NS + "productPropertyTextual1");
	
	//Property: productPropertyTextual2
	public static  String productPropertyTextual2 = (NS + "productPropertyTextual2");
	
	//Property: productPropertyTextual3
	public static  String productPropertyTextual3 = (NS + "productPropertyTextual3");
	
	//Property: productPropertyTextual4
	public static  String productPropertyTextual4 = (NS + "productPropertyTextual4");
	
	//Property: productPropertyTextual5
	public static  String productPropertyTextual5 = (NS + "productPropertyTextual5");
	
	//Property: productPropertyTextual6
	public static  String productPropertyTextual6 = (NS + "productPropertyTextual6");
	
	//Property: productPropertyNumeric1
	public static  String productPropertyNumeric1 = (NS + "productPropertyNumeric1");
	
	//Property: productPropertyNumeric2
	public static  String productPropertyNumeric2 = (NS + "productPropertyNumeric2");
	
	//Property: productPropertyNumeric3
	public static  String productPropertyNumeric3 = (NS + "productPropertyNumeric3");
	
	//Property: productPropertyNumeric4
	public static  String productPropertyNumeric4 = (NS + "productPropertyNumeric4");
	
	//Property: productPropertyNumeric5
	public static  String productPropertyNumeric5 = (NS + "productPropertyNumeric5");
	
	//Property: productPropertyNumeric6
	public static  String productPropertyNumeric6 = (NS + "productPropertyNumeric6");
	
	//Function creating the above numeric properties
	public static String getProductPropertyNumeric(int nr)
	{
		StringBuffer s = new StringBuffer();
		
		s.append(NS);
		s.append("productPropertyNumeric");
		s.append(nr);
		
		return s.toString();
	}
	
	//Function creating the above numeric properties as prefix version
	public static String getProductPropertyNumericPrefix(int nr)
	{
		StringBuffer s = new StringBuffer();
		
		s.append(PREFIX);
		s.append("productPropertyNumeric");
		s.append(nr);
		
		return s.toString();
	}
	
	//Function creating the above textual properties
	public static String getProductPropertyTextual(int nr)
	{
		StringBuffer s = new StringBuffer();
		
		s.append(NS);
		s.append("productPropertyTextual");
		s.append(nr);
		
		return s.toString();
	}
	
	//Function creating the above textual properties
	public static String getProductPropertyTextualPrefix(int nr)
	{
		StringBuffer s = new StringBuffer();
		
		s.append(PREFIX);
		s.append("productPropertyTextual");
		s.append(nr);
		
		return s.toString();
	}
	
	//Property: country
	public static  String country = (NS + "country");
	
	//Property: product
	public static  String product = (NS + "product");
	
	//Property: productType
	public static  String productType = (NS + "productType");
	
	//Property: vendor
	public static  String vendor = (NS + "vendor");
	
	//Property: price
	public static  String price = (NS + "price");
	
	//Data type USD
	public static  String USD = (NS + "USD");
	
	//Property: validFrom
	public static  String validFrom = (NS + "validFrom");
	
	//Property: validTo
	public static  String validTo = (NS + "validTo");
	
	//Property: deliveryDays
	public static  String deliveryDays = (NS + "deliveryDays");
	
	//Property: offerWebpage
	public static  String offerWebpage = (NS + "offerWebpage");
	
	//Property: reviewFor
	public static  String reviewFor = (NS + "reviewFor");
	
	//Property: reviewDate
	public static  String reviewDate = (NS + "reviewDate");
	
	//Property: rating1
	public static  String rating1 = (NS + "rating1");

	//Property: rating2
	public static  String rating2 = (NS + "rating2");

	//Property: rating3
	public static  String rating3 = (NS + "rating3");

	//Property: rating4
	public static  String rating4 = (NS + "rating4");
	
	//Function creating the above textual properties
	public static String getRating(int nr)
	{
		StringBuffer s = new StringBuffer();
		
		s.append(NS);
		s.append("rating");
		s.append(nr);
		
		return s.toString();
	}
	
	//Function creating the above textual properties
	public static String getRatingPrefix(int nr)
	{
		StringBuffer s = new StringBuffer();
		
		s.append(PREFIX);
		s.append("rating");
		s.append(nr);
		
		return s.toString();
	}
	
	public static String getStandardizationInstitution(int nr)
	{
		StringBuffer s = new StringBuffer();
		
		s.append(INST_NS);
		s.append("StandardizationInstitution");
		s.append(nr);
		
		return s.toString();
	}
	
	public static void setNewPrefixesAndNS(String vocabularyNS, String instanceNS, String vocabularyPrefix, String instancePrefix) {
		NS = vocabularyNS;
		
		PREFIX = vocabularyPrefix;
		
		uriMap = new HashMap<String, String>();
		
		INST_NS = instanceNS;
		
		INST_PREFIX = instancePrefix;
			
		//Resource type: Offer
		Offer = (NS+ "Offer");
		
		//Resource type: Vendor
		Vendor = (NS + "Vendor");
		
		//Resource type: Producer
		Producer = (NS + "Producer");
		
		//Resource type: ProductFeature
		ProductFeature = (NS + "ProductFeature");
		
		//Resource type: ProductCategory
		ProductCategory = (NS + "ProductCategory");
		
		//Resource type: ProductType
		ProductType = (NS + "ProductType");

		//Resource type: Product
		Product = (NS + "Product");
		
		//Property: productFeature
		productFeature = (NS + "productFeature");
		
		//Property: productCategory
		productCategory = (NS + "productCategory");
		
		//Property: producer
		producer = (NS + "producer");
		
		//Property: productStringTextual1
		productPropertyTextual1 = (NS + "productPropertyTextual1");
		
		//Property: productPropertyTextual2
		productPropertyTextual2 = (NS + "productPropertyTextual2");
		
		//Property: productPropertyTextual3
		productPropertyTextual3 = (NS + "productPropertyTextual3");
		
		//Property: productPropertyTextual4
		productPropertyTextual4 = (NS + "productPropertyTextual4");
		
		//Property: productPropertyTextual5
		productPropertyTextual5 = (NS + "productPropertyTextual5");
		
		//Property: productPropertyTextual6
		productPropertyTextual6 = (NS + "productPropertyTextual6");
		
		//Property: productPropertyNumeric1
		productPropertyNumeric1 = (NS + "productPropertyNumeric1");
		
		//Property: productPropertyNumeric2
		productPropertyNumeric2 = (NS + "productPropertyNumeric2");
		
		//Property: productPropertyNumeric3
		productPropertyNumeric3 = (NS + "productPropertyNumeric3");
		
		//Property: productPropertyNumeric4
		productPropertyNumeric4 = (NS + "productPropertyNumeric4");
		
		//Property: productPropertyNumeric5
		productPropertyNumeric5 = (NS + "productPropertyNumeric5");
		
		//Property: productPropertyNumeric6
		productPropertyNumeric6 = (NS + "productPropertyNumeric6");
		
		//Property: country
		country = (NS + "country");
		
		//Property: product
		product = (NS + "product");
		
		//Property: productType
		productType = (NS + "productType");
		
		//Property: vendor
		vendor = (NS + "vendor");
		
		//Property: price
		price = (NS + "price");
		
		//Data type USD
		USD = (NS + "USD");
		
		//Property: validFrom
		validFrom = (NS + "validFrom");
		
		//Property: validTo
		validTo = (NS + "validTo");
		
		//Property: deliveryDays
		deliveryDays = (NS + "deliveryDays");
		
		//Property: offerWebpage
		offerWebpage = (NS + "offerWebpage");
		
		//Property: reviewFor
		reviewFor = (NS + "reviewFor");
		
		//Property: reviewDate
		reviewDate = (NS + "reviewDate");
		
		//Property: rating1
		rating1 = (NS + "rating1");

		//Property: rating2
		rating2 = (NS + "rating2");

		//Property: rating3
		rating3 = (NS + "rating3");

		//Property: rating4
		rating4 = (NS + "rating4");
	}
}
