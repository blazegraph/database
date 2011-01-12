package benchmark.serializer;

import java.io.FileWriter;
import java.io.IOException;
import java.util.GregorianCalendar;
import java.util.Iterator;

import benchmark.generator.DateGenerator;
import benchmark.model.BSBMResource;
import benchmark.model.Offer;
import benchmark.model.Person;
import benchmark.model.Producer;
import benchmark.model.Product;
import benchmark.model.ProductFeature;
import benchmark.model.ProductType;
import benchmark.model.Review;
import benchmark.model.Vendor;
import benchmark.vocabulary.ISO3166;


public class XMLSerializer implements Serializer {
	private FileWriter fileWriter;
	private boolean forwardChaining;
	private long nrTriples;
	private static final String spacePrefix = "  ";
	
	public XMLSerializer(String file, boolean forwardChaining) {
		try{
			fileWriter = new FileWriter(file);
		} catch(IOException e){
			System.err.println("Could not open File for writing.");
			System.err.println(e.getMessage());
			System.exit(-1);
		}
		
		this.forwardChaining = forwardChaining;
		nrTriples = 0l;
		
		writeHeaderData();
	}
	
	private void writeHeaderData() {
		try {
			fileWriter.append("<?xml version=\"1.0\" encoding=\"ISO-8859-1\" standalone=\"yes\"?>");
			fileWriter.append(startTag(0,"bsbm_data"));
			fileWriter.append(startTag(1,"resources"));
		}catch(IOException e){
			System.err.println("Could not write into File!");
			System.err.println(e.getMessage());
			System.exit(-1);
		}
	}
	
	private void writeFooterData() {
		try {
			fileWriter.append(endTag(1,"resources"));
			fileWriter.append(endTag(0,"bsbm_data"));
		}catch(IOException e){
			System.err.println("Could not write into File!");
			System.err.println(e.getMessage());
			System.exit(-1);
		}
	}

	private String getPublisherTypeString(ObjectBundle bundle) {
		String publisher = bundle.getPublisher().toLowerCase();
		
		if(publisher.contains("standardizationinstitution"))
			return "dataFromStandardizationInstitution";
		else if(publisher.contains("vendor"))
			return "dataFromVendor";
		else if(publisher.contains("producer"))
			return "dataFromProducer";
		else if(publisher.contains("ratingsite"))
			return "dataFromRatingSite";
		else
			return null;
	}
	
	
	public void gatherData(ObjectBundle bundle) {
		Iterator<BSBMResource> it = bundle.iterator();

		String publisherType = getPublisherTypeString(bundle);
		if(publisherType==null) {
			System.err.println("Unknown publisher type");
			System.exit(-1);
		}
		
		
		
		try {
			fileWriter.append(startTagWA(2,publisherType,"id",new Integer(bundle.getPublisherNum()).toString()));
				
			while(it.hasNext())
			{
				BSBMResource obj = it.next();
	
				if(obj instanceof ProductType){
					fileWriter.append(convertProductType((ProductType)obj));
				}
				else if(obj instanceof Offer){
					fileWriter.append(convertOffer((Offer)obj));
				}
				else if(obj instanceof Product){
					fileWriter.append(convertProduct((Product)obj));
				}
				else if(obj instanceof Person){
					fileWriter.append(convertPerson((Person)obj));
				}
				else if(obj instanceof Producer){
					fileWriter.append(convertProducer((Producer)obj));
				}
				else if(obj instanceof ProductFeature){
					fileWriter.append(convertProductFeature((ProductFeature)obj));
				}
				else if(obj instanceof Vendor){
					fileWriter.append(convertVendor((Vendor)obj));
				}
				else if(obj instanceof Review){
					fileWriter.append(convertReview((Review)obj));
				}
			}
			
			fileWriter.append(endTag(2,publisherType));
			
		}catch(IOException e){
			System.err.println("Could not write into File!");
			System.err.println(e.getMessage());
			System.exit(-1);
		}
	}

	/*
	 * Converts the ProductType Object into an N-Triples String
	 * representation.
	 */
	private String convertProductType(ProductType pType)
	{
		StringBuffer result = new StringBuffer();
		result.append(startTagWA(3, "ProductType", "id", new Integer(pType.getNr()).toString()));
		
		//rdfs:label
		result.append(leafTag(4, "label", pType.getLabel()));
		
		//rdfs:comment
		result.append(leafTag(4, "comment", pType.getComment()));
		
		//rdfs:subClassOf
		if(pType.getParent()!=null)
		{
			Integer parent = pType.getParent().getNr();
			result.append(leafTag(4, "subClassOf", parent.toString()));

		}
		
		//dc:publisher
		result.append(leafTag(4, "publisher", "1"));
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(pType.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		result.append(leafTag(4, "publishDate", dateString));

		result.append(endTag(3, "ProductType"));
		
		return result.toString();
	}
	
	/*
	 * Converts the Offer Object into an N-Triples String
	 * representation.
	 */
	private String convertOffer(Offer offer)
	{
		StringBuffer result = new StringBuffer();
		//First the uriref for the subject
		result.append(startTagWA(3, "Offer", "id", new Integer(offer.getNr()).toString()));
		
		//bsbm:product
		result.append(leafTag(4, "product", offer.getProduct().toString()));
				
		
		//bsbm:vendor
		result.append(leafTag(4, "vendor", Integer.valueOf(offer.getVendor()).toString()));
		
		//bsbm:price
		result.append(leafTag(4, "price", offer.getPriceString()));
		
		//bsbm:validFrom
		GregorianCalendar validFrom = new GregorianCalendar();
		validFrom.setTimeInMillis(offer.getValidFrom());
		String validFromString = DateGenerator.formatDate(validFrom);
		result.append(leafTag(4, "validFrom", validFromString));
		
		//bsbm:validTo
		GregorianCalendar validTo = new GregorianCalendar();
		validTo.setTimeInMillis(offer.getValidTo());
		String validToString = DateGenerator.formatDate(validTo);
		result.append(leafTag(4, "validFrom", validToString));
		
		//bsbm:deliveryDays
		result.append(leafTag(4, "deliveryDays", offer.getDeliveryDays().toString()));
		
		//bsbm:offerWebpage
		result.append(leafTag(4, "offerWebpage", offer.getOfferWebpage()));
		
		//dc:publisher
		result.append(leafTag(4, "publisher", Integer.valueOf(offer.getVendor()).toString()));
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(offer.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		result.append(leafTag(4, "publishDate", dateString));
		
		result.append(endTag(3, "Offer"));
		
		return result.toString();
	}
	
	/*
	 * Converts the Product Object into an N-Triples String
	 * representation.
	 */
	private String convertProduct(Product product)
	{
		StringBuffer result = new StringBuffer();
		//First the uriref for the subject
		result.append(startTagWA(3, "Product", "id", new Integer(product.getNr()).toString()));

		//rdfs:label
		result.append(leafTag(4, "label", product.getLabel()));
		
		//rdfs:comment
		result.append(leafTag(4, "comment", product.getComment()));
		
		//rdf:type for product types 
		if(forwardChaining) {
			ProductType pt = product.getProductType();
			while(pt!=null) {
				result.append(leafTag(4, "type", new Integer(pt.getNr()).toString()));
				pt = pt.getParent();
			}
		}
		else {
			result.append(leafTag(4, "type", new Integer(product.getProductType().getNr()).toString()));
		}
		
		//bsbm:producer
		result.append(leafTag(4, "producer", new Integer(product.getProducer()).toString()));
		
		//bsbm:productPropertyNumeric
		Integer[] ppn = product.getProductPropertyNumeric();
		for(Integer i=0,j=1;i<ppn.length;i++,j++)
		{
			Integer value = ppn[i];
			if(value!=null)
				result.append(leafTagWA(4, "ProductPropertyNumeric", value.toString(), "nr", i.toString()));
		}
		
		//bsbm:productPropertyTextual
		String[] ppt = product.getProductPropertyTextual();
		for(Integer i=0,j=1;i<ppt.length;i++,j++)
		{
			String value = ppt[i];
			if(value!=null)
				result.append(leafTagWA(4, "ProductPropertyTextual", value, "nr", i.toString()));
		}
		
		//bsbm:productFeature
		Iterator<Integer> pf = product.getFeatures().iterator();
		while(pf.hasNext())
		{
			Integer value = pf.next();
			result.append(leafTag(4, "ProductFeature", value.toString()));

		}
		
		//dc:publisher
		result.append(leafTag(4, "publisher", new Integer(product.getProducer()).toString()));
		
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(product.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		result.append(leafTag(4, "publishDate", dateString));
		
		result.append(endTag(3, "Product"));
		
		return result.toString();
	}
	
	/*
	 * Converts the Person Object into an N-Triples String
	 * representation.
	 */
	private String convertPerson(Person person)
	{
		StringBuffer result = new StringBuffer();
		//First the uriref for the subject
		result.append(startTagWA(3, "Person", "id", new Integer(person.getNr()).toString()));
		
		//foaf:name
		result.append(leafTag(4, "name", person.getName()));
		
		//foaf:mbox_sha1sum
		result.append(leafTag(4, "mbox_sha1sum", person.getMbox_sha1sum()));
		
		//bsbm:country
		result.append(leafTag(4, "country", person.getCountryCode()));
		
		//dc:publisher
		result.append(leafTag(4, "publisher", person.getPublisher().toString()));
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(person.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		result.append(leafTag(4, "publishDate", dateString));
		
		result.append(endTag(3, "Person"));
		
		return result.toString();
	}
	
	/*
	 * Converts the Producer Object into an N-Triples String
	 * representation.
	 */
	private String convertProducer(Producer producer)
	{
		StringBuffer result = new StringBuffer();
		//First the uriref for the subject
		result.append(startTagWA(3, "Producer", "id", new Integer(producer.getNr()).toString()));
		
		//rdfs:label
		result.append(leafTag(4, "label", producer.getLabel()));
		
		//rdfs:comment
		result.append(leafTag(4, "comment", producer.getComment()));
		
		//foaf:homepage
		result.append(leafTag(4, "homepage", producer.getHomepage()));
		
		//bsbm:country
		result.append(leafTag(4, "country", producer.getCountryCode()));
		
		//dc:publisher
		result.append(leafTag(4, "publisher", new Integer(producer.getNr()).toString()));
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(producer.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		result.append(leafTag(4, "publishDate", dateString));

		result.append(endTag(3, "Producer"));
		
		return result.toString();
	}
	
	/*
	 * Converts the ProductFeature Object into an N-Triples String
	 * representation.
	 */
	private String convertProductFeature(ProductFeature pf)
	{
		StringBuffer result = new StringBuffer();
		//First the uriref for the subject
		result.append(startTagWA(3, "ProductFeature", "id", new Integer(pf.getNr()).toString()));
		
		//rdfs:label
		result.append(leafTag(4, "label", pf.getLabel()));
		
		//rdfs:comment
		result.append(leafTag(4, "comment", pf.getComment()));

		
		//dc:publisher
		result.append(leafTag(4, "publisher", pf.getPublisher().toString()));
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(pf.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		result.append(leafTag(4, "publishDate", dateString));
		
		result.append(endTag(3, "ProductFeature"));
		
		return result.toString();
	}
	
	/*
	 * Converts the Vendor Object into an N-Triples String
	 * representation.
	 */
	private String convertVendor(Vendor vendor)
	{
		StringBuffer result = new StringBuffer();
		//First the uriref for the subject
		result.append(startTagWA(3, "Vendor", "id", new Integer(vendor.getNr()).toString()));
		
		//rdfs:label
		result.append(leafTag(4, "label", vendor.getLabel()));
		
		//rdfs:comment
		result.append(leafTag(4, "comment", vendor.getComment()));
		
		//foaf:homepage
		result.append(leafTag(4, "homepage", vendor.getHomepage()));
		
		//bsbm:country
		result.append(leafTag(4, "country", vendor.getCountryCode()));
		
		//dc:publisher
		result.append(leafTag(4, "publisher",  new Integer(vendor.getNr()).toString()));
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(vendor.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		result.append(leafTag(4, "publishDate", dateString));

		result.append(endTag(3, "Vendor"));
		
		return result.toString();
	}
	
	
	/*
	 * Converts the Review Object into an N-Triples String
	 * representation.
	 */
	private String convertReview(Review review)
	{
		StringBuffer result = new StringBuffer();
		//First the uriref for the subject
		result.append(startTagWA(3, "Review", "id", new Integer(review.getNr()).toString()));

		//bsbm:reviewFor
		result.append(leafTag(4, "reviewfor", review.getProduct().toString()));
		
		//rev:reviewer
		result.append(leafTag(4, "reviewer", new Integer(review.getPerson()).toString()));
		
		//bsbm:reviewDate
		GregorianCalendar reviewDate = new GregorianCalendar();
		reviewDate.setTimeInMillis(review.getReviewDate());
		String reviewDateString = DateGenerator.formatDate(reviewDate);
		result.append(leafTag(4, "reviewDate", reviewDateString));
		
		//dc:title
		result.append(leafTag(4, "title", review.getTitle()));
		
		//rev:text
		result.append(leafTagWA(4, "text", review.getText(), "lang", ISO3166.language[review.getLanguage()]));
		
		//bsbm:ratingX
		Integer[] ratings = review.getRatings();
		for(Integer i=0,j=1;i<ratings.length;i++,j++)
		{
			Integer value = ratings[i];
			if(value!=null)
				result.append(leafTagWA(4, "rating", value.toString(), "nr", j.toString()));
		}
		
		//dc:publisher
		result.append(leafTag(4, "publisher", review.getPublisher().toString()));
		
		//dc:date
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(review.getPublishDate());
		String dateString = DateGenerator.formatDate(date);
		result.append(leafTag(4, "publishDate", dateString));
		
		result.append(endTag(3, "Review"));
		
		return result.toString();
	}
	
	private String endTag(int indentationSteps, String tag) {
		StringBuffer sb = new StringBuffer();
		for(int i=0;i<indentationSteps;i++)
			sb.append(spacePrefix);
		
		sb.append("</");
		sb.append(tag);
		sb.append(">\n");
		return sb.toString();
	}
	
	private String startTag(int indentationSteps, String tag) {
		StringBuffer sb = new StringBuffer();
		for(int i=0;i<indentationSteps;i++)
			sb.append(spacePrefix);
		
		sb.append("<");
		sb.append(tag);
		sb.append(">\n");
		return sb.toString();
	}
	
	private String startTagWA(int indentationSteps, String tag, String attribute, String attributeContent) {
		StringBuffer sb = new StringBuffer();
		for(int i=0;i<indentationSteps;i++)
			sb.append(spacePrefix);
		
		sb.append("<");
		sb.append(tag);
		sb.append(" ");
		sb.append(attribute);
		sb.append("=\"");
		sb.append(attributeContent);
		sb.append("\">\n");
		return sb.toString();
	}
	
	private String leafTag(int indentationSteps, String tag, String content) {
		StringBuffer sb = new StringBuffer();
		for(int i=0;i<indentationSteps;i++)
			sb.append(spacePrefix);
		
		sb.append("<");
		sb.append(tag);
		sb.append(">");
		sb.append(content);
		sb.append("</");
		sb.append(tag);
		sb.append(">\n");
		return sb.toString();
	}
	
	private String leafTagWA(int indentationSteps, String tag, String content, String attribute, String attributeContent) {
		StringBuffer sb = new StringBuffer();
		for(int i=0;i<indentationSteps;i++)
			sb.append(spacePrefix);
		
		sb.append("<");
		sb.append(tag);
		sb.append(" ");
		sb.append(attribute);
		sb.append("=\"");
		sb.append(attributeContent);
		sb.append("\">");
		sb.append(content);
		sb.append("</");
		sb.append(tag);
		sb.append(">\n");
		return sb.toString();
	}

	public void serialize() {
		//Close files
		try {
			writeFooterData();
			fileWriter.flush();
			fileWriter.close();
		} catch(IOException e) {
			System.err.println(e.getMessage());
			System.exit(-1);
		}
	}

	public Long triplesGenerated() {
		return nrTriples;
	}

}
