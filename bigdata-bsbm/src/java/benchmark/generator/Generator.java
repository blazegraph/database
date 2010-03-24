/*
 * Copyright (C) 2008 Andreas Schultz
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package benchmark.generator;

import benchmark.model.*;
import benchmark.serializer.*;

import java.util.*;

import benchmark.vocabulary.*;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class Generator {
	//Program parameters and default values
	private static int productCount = 100;
	private static boolean forwardChaining = false;
	private static String outputDirectory = "td_data";
	private static String outputFileName = "dataset";
	private static String serializerType = "nt"; 
	
	//Ratios of different Resources
	static final int productsVendorsRatio = 100;
	
	static final int avgReviewsPerProduct=10;
	static final int avgReviewsPerPerson=20;
	static final int avgReviewsPerRatingSite=10000;
	static final int avgProductsPerProducer = 50;
	static final int avgOffersPerProduct = 20;
	static final int avgOffersPerVendor = productsVendorsRatio * avgOffersPerProduct;
	
	static final String dictionary1File = "titlewords.txt";
	static final String dictionary2File = "titlewords.txt";
	static final String dictionary3File = "givennames.txt";
	
	static final Random seedGenerator = new Random(53223436L);
	
	static TextGenerator dictionary1;
	static TextGenerator dictionary2;
	static TextGenerator dictionary3;
	 
	static GregorianCalendar today = new GregorianCalendar(2008,5,20);//Date of 2008-06-20
	
	static int producerCount;
	static int offerCount;
	static int reviewCount;
	static int ratingSiteCount;
	
	static boolean namedGraph;

	private static ArrayList<ProductType> productTypeLeaves;
	private static ArrayList<ProductType> productTypeNodes;
	private static ArrayList<Integer> producerOfProduct;//saves producer-product relationship
	private static ArrayList<Integer> vendorOfOffer;//saves vendor-offer relationship
	private static ArrayList<Integer> ratingsiteOfReview;//saves review-ratingSite relationship
	private static HashMap<String,Integer> wordList;//Word list for the Test driver
	
	private static Serializer serializer;
	
	private static File outputDir;

	//Set parameters
	public static void init()
	{
		offerCount = productCount * avgOffersPerProduct;

		reviewCount = avgReviewsPerProduct * productCount;
		
		producerOfProduct = new ArrayList<Integer>();
		producerOfProduct.add(0);
		vendorOfOffer = new ArrayList<Integer>();
		vendorOfOffer.add(0);
		ratingsiteOfReview = new ArrayList<Integer>();
		ratingsiteOfReview.add(0);
		
		Generator.serializer = getSerializer(serializerType);
		if(serializer==null) {
			System.err.println("Invalid Serializer chosen.");
			System.exit(-1);
		}

		namedGraph = isNamedGraphSerializer();
		
		Generator.outputDir = new File(outputDirectory);
		outputDir.mkdirs();
		wordList = new HashMap<String, Integer>();
		
		dictionary1 = new TextGenerator(dictionary1File, seedGenerator.nextLong());
		dictionary2 = new TextGenerator(dictionary2File, seedGenerator.nextLong());
		dictionary3 = new TextGenerator(dictionary3File, seedGenerator.nextLong());
		System.out.println("");
	}
	
	private static boolean isNamedGraphSerializer() {
		if(serializer instanceof TriG)
			return true;
		else
			return false;
	}
	
	private static Serializer getSerializer(String type) {
		String t = type.toLowerCase();
		if(t.equals("nt"))
			return new NTriples(outputFileName + ".nt", forwardChaining);
		else if(t.equals("trig"))
			return new TriG(outputFileName + ".trig", forwardChaining);
		else if(t.equals("ttl"))
			return new Turtle(outputFileName + ".ttl", forwardChaining);
		else if(t.equals("xml"))
			return new XMLSerializer(outputFileName + ".xml", forwardChaining);
		else if(t.equals("sql"))
			return new SQLSerializer(outputFileName, forwardChaining, "benchmark");
		else if(t.equals("virt"))
			return new VirtSerializer(outputFileName, forwardChaining);
		else
			return null;
	}
	
	/*
	 * Write data for the Test Driver to disk
	 */
	public static void writeTestDriverData() {
		//Product Type hierarchy to File outputDir/pth.dat
		File pth = new File(outputDir, "pth.dat");
		ObjectOutputStream productTypeOutput;
		try {
			pth.createNewFile();
			productTypeOutput = new ObjectOutputStream(new FileOutputStream(pth, false));
			productTypeOutput.writeObject(productTypeLeaves.toArray(new ProductType[0]));
		} catch(IOException e) {
			System.err.println("Could not open or create file " + pth.getAbsolutePath());
			System.err.println(e.getMessage());
			System.exit(-1);
		}
		
		//Product-Producer Relationships in outputDir/pp.dat
		File pp = new File(outputDir, "pp.dat");
		ObjectOutputStream productProducerOutput;
		try {
			pp.createNewFile();
			productProducerOutput = new ObjectOutputStream(new FileOutputStream(pp, false));
			productProducerOutput.writeObject(producerOfProduct.toArray(new Integer[0]));
		} catch(IOException e) {
			System.err.println("Could not open or create file " + pp.getAbsolutePath());
			System.err.println(e.getMessage());
			System.exit(-1);
		}
		
		//Product-Producer Relationships in outputDir/pp.dat
		File vo = new File(outputDir, "vo.dat");
		ObjectOutputStream offerVendorOutput;
		try {
			vo.createNewFile();
			offerVendorOutput = new ObjectOutputStream(new FileOutputStream(vo, false));
			offerVendorOutput.writeObject(vendorOfOffer.toArray(new Integer[0]));
		} catch(IOException e) {
			System.err.println("Could not open or create file " + vo.getAbsolutePath());
			System.err.println(e.getMessage());
			System.exit(-1);
		}
	
		//Review-Rating Site Relationships in outputDir/rr.dat
		File rr = new File(outputDir, "rr.dat");
		ObjectOutputStream reviewRatingsiteOutput;
		try {
			rr.createNewFile();
			reviewRatingsiteOutput = new ObjectOutputStream(new FileOutputStream(rr, false));
			reviewRatingsiteOutput.writeObject(ratingsiteOfReview.toArray(new Integer[0]));
		} catch(IOException e) {
			System.err.println("Could not open or create file " + rr.getAbsolutePath());
			System.err.println(e.getMessage());
			System.exit(-1);
		}
		
		//Current date scaling data and words of Product labels in outputDir/cdlw.dat
		File cdlw = new File(outputDir, "cdlw.dat");
		ObjectOutputStream currentDateAndLabelWordsOutput;
		try {
			cdlw.createNewFile();
			currentDateAndLabelWordsOutput = new ObjectOutputStream(new FileOutputStream(cdlw, false));
			currentDateAndLabelWordsOutput.writeInt(productCount);
			currentDateAndLabelWordsOutput.writeInt(reviewCount);
			currentDateAndLabelWordsOutput.writeInt(offerCount);
			currentDateAndLabelWordsOutput.writeObject(today);
			currentDateAndLabelWordsOutput.writeObject(wordList);
		} catch(IOException e) {
			System.err.println("Could not open or create file " + cdlw.getAbsolutePath());
			System.err.println(e.getMessage());
			System.exit(-1);
		}
	}
	
	/*
	 * Calculate the number of levels and the branching factors
	 * for the Product Type Hierarchy
	 */
	private static int[] calcBranchingFactors(long scalefactor)
	{
		float logSF = (float)Math.log10(scalefactor);

		//depth = log10(scale factor)/2 + 1
		int depth = Math.round((logSF) / 2) + 1;
		
		int[] branchingFactors = new int[depth];
		
		branchingFactors[0] = 2 * Math.round(logSF);

		int[] temp = { 2, 4, 8};
		for(int i=1;i<depth;i++)
		{
			if(i+1 < depth)
				branchingFactors[i] = 8;
			else {
				int value = temp[Math.round(logSF*3/2+1)%3];
				branchingFactors[i] = value;
			}
		}
		return branchingFactors;
	}
	
	/*
	 * Creates the Product Types and orders them as a tree.
	 */
	public static void createProductTypeHierarchy()
	{
		System.out.println("Generating Product Type Hierarchy...");
		DateGenerator publishDateGen = new DateGenerator(new GregorianCalendar(2000,05,20),new GregorianCalendar(2000,06,23),seedGenerator.nextLong());
		ValueGenerator valueGen = new ValueGenerator(seedGenerator.nextLong());
		
		ObjectBundle bundle = new ObjectBundle(serializer);
		
		//Calculate branch factors for inner nodes
		int[] branchFt;
		if(productCount>=10)
			branchFt = calcBranchingFactors(productCount);
		else
			branchFt = calcBranchingFactors(10);

		productTypeLeaves = new ArrayList<ProductType>();
		productTypeNodes = new ArrayList<ProductType>();
		
		LinkedList<ProductType> typeQueue = new LinkedList<ProductType>();
		ProductType root = new ProductType(1,"Thing", "The Product Type of all Products", null);
		if(!namedGraph) {
			root.setPublisher(1);
			root.setPublishDate(publishDateGen.randomDateInMillis());
			String publisher = BSBM.getStandardizationInstitution(1);
			bundle.setPublisher(publisher);
			bundle.setPublisherNum(1);
		}
		else {
			String publisher = BSBM.getStandardizationInstitution(1);
			bundle.setPublisher("<" + publisher + ">");
			bundle.setPublishDate(publishDateGen.randomDateInMillis());
			bundle.setGraphName("<" + publisher + "/Graph-" + DateGenerator.formatDate(bundle.getPublishDate()) + ">");
		}
		productTypeNodes.add(root);
		
		typeQueue.offer(root);
		bundle.add(root);
		
		int nr = 1;
		while(!typeQueue.isEmpty())
		{
			ProductType ptype = typeQueue.poll();
			
			int depth = ptype.getDepth();
		
			for(int i=0;i<branchFt[ptype.getDepth()];i++)
			{
				nr++;
				
				int labelNrWords = valueGen.randomInt(1, 3);
				String label = dictionary1.getRandomSentence(labelNrWords);
				
				int commentNrWords = valueGen.randomInt(20, 50);
				String comment = dictionary2.getRandomSentence(commentNrWords);
				
				ProductType newType = new ProductType(nr, label, comment, ptype);
				
				if(!namedGraph) {
					newType.setPublisher(1);
					newType.setPublishDate(publishDateGen.randomDateInMillis());
				}
				
				bundle.add(newType);
				
				//If new Product Type is leaf
				if(depth==branchFt.length-1) {
					productTypeLeaves.add(newType);
				} else {
					productTypeNodes.add(newType);
					typeQueue.offer(newType);
				}
			}
		}
		bundle.commitToSerializer();
		System.out.println("Product Type Hierarchy of depth " + branchFt.length + " with " + (productTypeLeaves.size()+productTypeNodes.size()) + " Product Types generated.\n");
	}
	
	/*
	 * Create Product Features
	 */
	public static void createProductFeatures()
	{
		System.out.println("Generating Product Features...");
		ObjectBundle bundle = new ObjectBundle(serializer);
		ValueGenerator valueGen = new ValueGenerator(seedGenerator.nextLong());
		DateGenerator publishDateGen = new DateGenerator(new GregorianCalendar(2000,05,20),new GregorianCalendar(2000,06,23),seedGenerator.nextLong());
		
		//Compute count range of Features per Product Type for every depth
		int depth = productTypeLeaves.get(0).getDepth();

		int[] featureFrom = new int[depth];
		int[] featureTo = new int[depth];
		
		//Depth 1 always 5
		featureFrom[0] = 5;
		featureTo[0] = 5;
		
		// -1 because of depth 1
		int depthSum = depth * (depth+1) / 2 - 1;
		
		for(int i=2; i<=depth; i++)
		{
			featureFrom[i-1] = 35 * i / depthSum;
			featureTo[i-1] = 75 * i / depthSum;
		}

		//Product Feature Nr.
		int productFeatureNr = 1;
		
		//First the Inner nodes
		Iterator<ProductType> it = productTypeNodes.iterator();
		it.next();//Ignore the root
		
		if(namedGraph) {
			String publisher = BSBM.getStandardizationInstitution(2);
			bundle.setPublisher("<" + publisher + ">");
			bundle.setPublishDate(publishDateGen.randomDateInMillis());
			bundle.setGraphName("<" + publisher + "/Graph-" + DateGenerator.formatDate(bundle.getPublishDate()) + ">");
		}
		else {
			String publisher = BSBM.getStandardizationInstitution(2);
			bundle.setPublisher(publisher);
			bundle.setPublisherNum(2);
		}
			
		
		while(it.hasNext())
		{
			ProductType pt = it.next();
			int from = featureFrom[pt.getDepth()];
			int to = featureTo[pt.getDepth()];
			
			int count = valueGen.randomInt(from, to);
			Vector<Integer> features = new Vector<Integer>(count);
			
			for(int i=0;i<count;i++){
				int labelNrWords = valueGen.randomInt(1, 3);
				String label = dictionary1.getRandomSentence(labelNrWords);
				
				int commentNrWords = valueGen.randomInt(20, 50);
				String comment = dictionary2.getRandomSentence(commentNrWords);
				
				ProductFeature pf = new ProductFeature(productFeatureNr,label,comment);
				if(!namedGraph) {
					pf.setPublisher(1);
					pf.setPublishDate(publishDateGen.randomDateInMillis());
				}
				
				features.add(productFeatureNr);
				
				bundle.add(pf);
				
				productFeatureNr++;
			}
			pt.setFeatures(features);
		}
		
		//Now the Leaves
		it = productTypeLeaves.iterator();
		
		while(it.hasNext())
		{
			ProductType pt = it.next();
			int from = featureFrom[pt.getDepth()-1];
			int to = featureTo[pt.getDepth()-1];

			int count = valueGen.randomInt(from, to);
			Vector<Integer> features = new Vector<Integer>(count);
			
			for(int i=0;i<count;i++){
				int labelNrWords = valueGen.randomInt(1, 3);
				String label = dictionary1.getRandomSentence(labelNrWords);
				
				int commentNrWords = valueGen.randomInt(20, 50);
				String comment = dictionary2.getRandomSentence(commentNrWords);
				
				ProductFeature pf = new ProductFeature(productFeatureNr,label,comment);
				
				if(!namedGraph) {
					pf.setPublisher(1);
					pf.setPublishDate(publishDateGen.randomDateInMillis());
				}
				
				features.add(productFeatureNr);
				
				bundle.add(pf);
				
				productFeatureNr++;
			}
			pt.setFeatures(features);
		}
		bundle.commitToSerializer();
		System.out.println((productFeatureNr-1) + " Product Features generated.\n");
	}

	/*
	 * Creates the Producers and their Products
	 */
	public static void createProducerData()
	{
		System.out.println("Generating Producers and Products...");
		DateGenerator publishDateGen = new DateGenerator(new GregorianCalendar(2000,07,20),new GregorianCalendar(2005,06,23),seedGenerator.nextLong());
		ValueGenerator valueGen = new ValueGenerator(seedGenerator.nextLong());
		RandomBucket countryGen = createCountryGenerator(seedGenerator.nextLong());
		
		
		NormalDistGenerator productCountGen = new NormalDistGenerator(3,1,avgProductsPerProducer,seedGenerator.nextLong());
		Integer productNr = 1;
		
		Integer producerNr = 1;
		
		ObjectBundle bundle = new ObjectBundle(serializer);
		
		while(productNr<=productCount)
		{
			//Generate Producer data
			int labelNrWords = valueGen.randomInt(1, 3);
			String label = dictionary1.getRandomSentence(labelNrWords);
			
			int commentNrWords = valueGen.randomInt(20, 50);
			String comment = dictionary2.getRandomSentence(commentNrWords);
			
			String homepage = TextGenerator.getProducerWebpage(producerNr);
			
			String country = (String)countryGen.getRandom();
			
			Producer p = new Producer(producerNr,label,comment,homepage,country);
			
			//Generate Publisher data
			if(!namedGraph) {
				p.setPublisher(producerNr);
				p.setPublishDate(publishDateGen.randomDateInMillis());
				bundle.setPublisher(p.toString());
				bundle.setPublisherNum(p.getNr());
			}
			else {
				bundle.setPublisher(p.toString());
				bundle.setPublishDate(publishDateGen.randomDateInMillis());
				bundle.setGraphName("<" + Producer.getProducerNS(p.getNr()) + "Graph-" + DateGenerator.formatDate(bundle.getPublishDate()) + ">");
				bundle.setPublisherNum(p.getNr());
			}
			
			bundle.add(p);
			
			//Now generate Products for this Producer
			int hasNrProducts = productCountGen.getValue();
			if(productNr+hasNrProducts-1 > productCount)
				hasNrProducts = productCount - productNr + 1;
			
			createProductsOfProducer(bundle, producerNr, productNr, hasNrProducts);
			
			//All data for current producer generated -> commit (Important for NG-Model).
			bundle.commitToSerializer();
			
			productNr += hasNrProducts;
			producerOfProduct.add(productNr-1);
			producerNr++;
		}
		System.out.println((producerNr - 1) + " Producers and " + (productNr - 1) + " Products have been generated.\n");
	}

	/*
	 * Creates the Products of the specified producer
	 */
	private static void createProductsOfProducer(ObjectBundle bundle, Integer producer, Integer productNr, Integer hasNrProducts)
	{
		DateGenerator publishDateGen = new DateGenerator(new GregorianCalendar(2000,9,20),new GregorianCalendar(2006,12,23),seedGenerator.nextLong());
		//We want to record used words for product labels
		dictionary1.activateLogging(wordList);
		ValueGenerator valueGen = new ValueGenerator(seedGenerator.nextLong());
		NormalDistRangeGenerator productTypeBroker = new NormalDistRangeGenerator(0,1,productTypeLeaves.size(),2, seedGenerator.nextLong());
		NormalDistRangeGenerator numPropertyGen = new NormalDistRangeGenerator(0,1,2000,2, seedGenerator.nextLong());
		
		//For assigning a type out of 3 possible types
		RandomBucket productPropertyTypeGen = new RandomBucket(3,seedGenerator.nextLong());
		productPropertyTypeGen.add(40, new Integer(1));
		productPropertyTypeGen.add(20, new Integer(2));
		productPropertyTypeGen.add(40, new Integer(3));
		
		//For choosing ProductFeatures and ProductProperties
		RandomBucket true25 = new RandomBucket(2,seedGenerator.nextLong());
		true25.add(75, new Boolean(false));
		true25.add(25, new Boolean(true));
		
		//For choosing ProductProperties
		RandomBucket true50 = new RandomBucket(2,seedGenerator.nextLong());
		true50.add(50, new Boolean(false));
		true50.add(50, new Boolean(true));

		for(int nr = productNr; nr<productNr+hasNrProducts;nr++)
		{
			//Generate Product data
			int labelNrWords = valueGen.randomInt(1, 3);
			String label = dictionary1.getRandomSentence(labelNrWords);
			
			int commentNrWords = valueGen.randomInt(50, 150);
			String comment = dictionary2.getRandomSentence(commentNrWords);
			
			ProductType productType = productTypeLeaves.get(productTypeBroker.getValue()-1);
	
		
			int productPropertyType = (Integer)productPropertyTypeGen.getRandom();
	
			//Generating Product Properties
			Integer[] numProperties = new Integer[6];
			String[] textProperties = new String[6]; 
			for(int i=0; i<3; i++){
				numProperties[i] = numPropertyGen.getValue();
				int nrWords = valueGen.randomInt(3, 15);
				textProperties[i] = dictionary2.getRandomSentence(nrWords);
			}
			
			//ProductProperty4
			numProperties[3] = null;
			textProperties[3] = null;
			boolean hasNum = false;
			boolean hasText = false;
			
			if(productPropertyType==1)
				hasNum = hasText = true;

			if(productPropertyType==2) {
				hasNum = (Boolean)true50.getRandom();
				hasText = (Boolean)true50.getRandom();
			}
			
			if(hasNum)
				numProperties[3] = numPropertyGen.getValue();
			
			if(hasText)
				textProperties[3] = dictionary2.getRandomSentence(valueGen.randomInt(3, 15));
			
			//ProductProperty5
			numProperties[4] = null;
			textProperties[4] = null;
			hasNum = false;
			hasText = false;
			
			if(productPropertyType==1)
				hasNum = hasText = true;
			
			if(productPropertyType==2 || productPropertyType==3) {
				hasNum = (Boolean)true25.getRandom();
				hasText = (Boolean)true25.getRandom();
			}
			
			if(hasNum)
				numProperties[4] = numPropertyGen.getValue();
			
			if(hasText)
				textProperties[4] = dictionary2.getRandomSentence(valueGen.randomInt(3, 15));
			
			//ProductProperty6
			numProperties[5] = null;
			textProperties[5] = null;
			if(productPropertyType==3) {
				if((Boolean)true50.getRandom())
					numProperties[5] = numPropertyGen.getValue();
				if((Boolean)true50.getRandom()) {
					int nrWords = valueGen.randomInt(3, 15);
					textProperties[5] = dictionary2.getRandomSentence(nrWords);
				}
			}
			
			//Assigning Product Features
			Vector<Integer> features = new Vector<Integer>();
			ProductType tempPT = productType;
			while(tempPT.getParent()!=null)
			{
				Iterator<Integer> it = tempPT.getFeatures().iterator();
				while(it.hasNext()) {
					Integer feature = it.next();
					if((Boolean)true25.getRandom())
						features.add(feature);
				}
				
				tempPT = tempPT.getParent();
			}
			
			Product p = new Product(nr,label,comment, productType, producer);
			
			p.setProductPropertyNumeric(numProperties);
			p.setProductPropertyTextual(textProperties);
			p.setFeatures(features);
				
			//Generate Publisher data
			if(!namedGraph) {
				p.setPublisher(producer);
				p.setPublishDate(publishDateGen.randomDateInMillis());
			}
			
			bundle.add(p);	
		}
		dictionary1.deactivateLogging();
	}

	/*
	 * Returns the ProducerNr of given ProductNr
	 */
	public static Integer getProducerOfProduct(Integer productNr) {
		Integer producerNr = Collections.binarySearch(producerOfProduct, productNr);
		if(producerNr<0)
			producerNr = - producerNr - 1;
		
		return producerNr;
	}
	
	/*
	 * Creates a country generator
	 */
	public static RandomBucket createCountryGenerator(Long seed)
	{
		RandomBucket countryGen = new RandomBucket(10, seed);
		
		countryGen.add(40, "US");
		countryGen.add(10, "GB");
		countryGen.add(10, "JP");
		countryGen.add(10, "CN");
		countryGen.add(5, "DE");
		countryGen.add(5, "FR");
		countryGen.add(5, "ES");
		countryGen.add(5, "RU");
		countryGen.add(5, "KR");
		countryGen.add(5, "AT");
		
		return countryGen;
	}
	
	/*
	 * Creates the Vendors
	 */
	public static void createVendorData()
	{
		System.out.println("Generating Vendors and their Offers...");
		DateGenerator publishDateGen = new DateGenerator(new GregorianCalendar(2000,9,20),new GregorianCalendar(2006,12,23),seedGenerator.nextLong());
		ValueGenerator valueGen = new ValueGenerator(seedGenerator.nextLong());
		RandomBucket countryGen = createCountryGenerator(seedGenerator.nextLong());
		NormalDistGenerator offerCountGenerator = new NormalDistGenerator(3,1,avgOffersPerVendor,seedGenerator.nextLong());
		
		
		ObjectBundle bundle = new ObjectBundle(serializer);
		
		Integer offerNr = 1;
		Integer vendorNr = 1;
		
		while(offerNr<=offerCount)
		{
			//Generate Vendor data
			int labelNrWords = valueGen.randomInt(1, 3);
			String label = dictionary1.getRandomSentence(labelNrWords);
			
			int commentNrWords = valueGen.randomInt(20, 50);
			String comment = dictionary2.getRandomSentence(commentNrWords);
			
			String homepage = TextGenerator.getVendorWebpage(vendorNr);
			
			String country = (String)countryGen.getRandom();
			
			Vendor v = new Vendor(vendorNr,label,comment,homepage,country);
			
			//Generate Publisher data
			if(!namedGraph) {
				v.setPublisher(vendorNr);
				v.setPublishDate(publishDateGen.randomDateInMillis(today.getTimeInMillis()-(97*DateGenerator.oneDayInMillis), today.getTimeInMillis()));
				bundle.setPublisher(v.toString());
				bundle.setPublisherNum(v.getNr());
			}
			else {
				bundle.setPublisher(v.toString());
				bundle.setPublishDate(publishDateGen.randomDateInMillis());
				bundle.setGraphName("<" + Vendor.getVendorNS(v.getNr()) + "Graph-" + DateGenerator.formatDate(bundle.getPublishDate()) + ">");
				bundle.setPublisherNum(v.getNr());
			}
			
			bundle.add(v);
			
			//Get number of offers for this Vendor
			Integer offerCountVendor = offerCountGenerator.getValue();
			if(offerNr+offerCountVendor-1 > offerCount)
				offerCountVendor = offerCount - offerNr + 1;
			
			createOffersOfVendor(bundle, vendorNr, offerNr, offerCountVendor, valueGen);
			
			//All data for current producer generated -> commit (Important for NG-Model).
			bundle.commitToSerializer();
			
			offerNr += offerCountVendor;
			vendorOfOffer.add(offerNr-1);
			vendorNr++;
		}
		System.out.println((vendorNr-1) + " Vendors and " + (offerNr - 1) + " Offers have been generated.\n");
	}

	/*
	 * Creates the offers for a product
	 */
	public static void createOffersOfVendor(ObjectBundle bundle, Integer vendor, Integer offerNr, Integer hasNrOffers, ValueGenerator valueGen)
	{
		NormalDistRangeGenerator deliveryDaysGen = new NormalDistRangeGenerator(2,1,21,14.2,seedGenerator.nextLong());
		NormalDistRangeGenerator productNrGen = new NormalDistRangeGenerator(2,1,productCount,4,seedGenerator.nextLong());
		DateGenerator dateGen = new DateGenerator(seedGenerator.nextLong());
		
		for(int nr=offerNr;nr<offerNr+hasNrOffers;nr++)
		{
			int product = productNrGen.getValue();
			double price = valueGen.randomDouble(5, 10000);
			Long publishDate;
			if(namedGraph)
				publishDate = bundle.getPublishDate();
			else
				publishDate = dateGen.randomDateInMillis(today.getTimeInMillis()-(97*DateGenerator.oneDayInMillis), today.getTimeInMillis());
			
			Long validFrom = publishDate - (valueGen.randomInt(0, 90)*DateGenerator.oneDayInMillis);
			Long validTo = publishDate + (valueGen.randomInt(7, 90)*DateGenerator.oneDayInMillis);
			int deliveryDays = deliveryDaysGen.getValue();
			String webpage = Vendor.getVendorNS(vendor) + "Offer" + nr + "/";
			
			Offer offer = new Offer(nr, product, vendor, price, validFrom, validTo, deliveryDays, webpage);
			
			if(!namedGraph) {
				offer.setPublishDate(publishDate);
				offer.setPublisher(vendor);
			}
			
			bundle.add(offer);
		}
	}

	/*
	 * Creates the Reviewers
	 */
	public static void createRatingSiteData()
	{
		System.out.println("Generating RatingSite Data: Reviewers and Reviews... ");
		DateGenerator publishDateGen = new DateGenerator(new GregorianCalendar(2008,5,20),new GregorianCalendar(2008,8,23),seedGenerator.nextLong());
		ValueGenerator valueGen = new ValueGenerator(seedGenerator.nextLong());
		RandomBucket countryGen = createCountryGenerator(seedGenerator.nextLong());
		
		//For Review Generation
		DateGenerator reviewDateGen = new DateGenerator(182,today,seedGenerator.nextLong());
		RandomBucket true70 = new RandomBucket(2, seedGenerator.nextLong());
		NormalDistRangeGenerator productNrGen = new NormalDistRangeGenerator(2,1,productCount,4,seedGenerator.nextLong());
		true70.add(70, new Boolean(true));
		true70.add(30, new Boolean(false));

		
		NormalDistGenerator reviewCountPRSGen = new NormalDistGenerator(3,1, avgReviewsPerRatingSite ,seedGenerator.nextLong());
		NormalDistGenerator reviewCountPPGen = new NormalDistGenerator(3,1, avgReviewsPerPerson ,seedGenerator.nextLong());
		
		Integer reviewNr = 1;
		Integer personNr = 1;
		Integer ratingSiteNr = 1;
		
		ObjectBundle bundle = new ObjectBundle(serializer);
		
		while(reviewNr<=reviewCount)
		{
			//Get number of reviews for this Rating Site
			Integer reviewCountRatingSite = reviewCountPRSGen.getValue();
			if(reviewNr+reviewCountRatingSite > reviewCount)
				reviewCountRatingSite = reviewCount - reviewNr + 1;
			
			//Generate provenance data for this rating site
			if(namedGraph) {
				bundle.setPublisher(RatingSite.getURIref(ratingSiteNr));
				bundle.setPublishDate(publishDateGen.randomDateInMillis());
				bundle.setGraphName("<" + RatingSite.getRatingSiteNS(ratingSiteNr) + "Graph-" + DateGenerator.formatDate(bundle.getPublishDate()) + ">");
				bundle.setPublisherNum(ratingSiteNr);
			} 
			else {
				bundle.setPublisher(RatingSite.getURIref(ratingSiteNr));
				bundle.setPublisherNum(ratingSiteNr);
			}
			//Now generate persons and reviews
			Integer maxReviewForRatingSite = reviewNr+reviewCountRatingSite;

			while(reviewNr < maxReviewForRatingSite)
			{		
				//Generate Person data
				String name = dictionary3.getRandomSentence(1);
				
				String country = (String)countryGen.getRandom();
				
				String mbox_sha1 = valueGen.randomSHA1();
				
				Person p = new Person(personNr,name,country,mbox_sha1);
				
				//Generate Publisher data
				if(!namedGraph) {
					p.setPublishDate(publishDateGen.randomDateInMillis());
				}
				//needed for qualified name
				p.setPublisher(ratingSiteNr);
			
				bundle.add(p);
			
				//Now generate Reviews for this Person
				Integer reviewCountPerson = reviewCountPPGen.getValue0();
				if(reviewNr+reviewCountPerson > maxReviewForRatingSite)
					reviewCountPerson = maxReviewForRatingSite - reviewNr;
				
				createReviewsOfPerson(bundle, p, reviewNr, reviewCountPerson, valueGen, reviewDateGen,
									  productNrGen, publishDateGen, true70);
				personNr++;
				reviewNr += reviewCountPerson;
			}
			
			//All data for current producer generated -> commit (Important for NG-Model).
			bundle.commitToSerializer();
			
			ratingsiteOfReview.add(reviewNr-1);
			ratingSiteNr++;
		}
		System.out.println((ratingSiteNr - 1) + " Rating Sites with " + (personNr - 1) + " Persons and " + (reviewNr-1) + " Reviews have been generated.\n");
	}
	
	
	/*
	 * Creates the reviews for a person
	 */
	private static void createReviewsOfPerson(ObjectBundle bundle, Person person, Integer reviewNr, Integer count,
			ValueGenerator valueGen, DateGenerator dateGen, NormalDistRangeGenerator prodNrGen,
			DateGenerator publishDateGen, RandomBucket true70)
	{
		for(int i=0;i<count;i++)
		{
			int product = prodNrGen.getValue();
			int producerOfProduct = getProducerOfProduct(product);
			int personNr = person.getNr();
			Long reviewDate = dateGen.randomDateInMillis(today.getTimeInMillis()-DateGenerator.oneDayInMillis*365,today.getTimeInMillis());
			int titleCount = valueGen.randomInt(4, 15);
			String title = dictionary2.getRandomSentence(titleCount);
			int textCount = valueGen.randomInt(50, 200);
			String text = dictionary2.getRandomSentence(textCount);
			int language = ISO3166.countryCodes.get(person.getCountryCode());
			
			Integer[] ratings = new Integer[4];
			
			for(int j=0;j<4;j++)
				if((Boolean)true70.getRandom())
					ratings[j] = valueGen.randomInt(1, 10);
				else
					ratings[j] = null;
			
			Review review = new Review(reviewNr, product, personNr, reviewDate, title, text, ratings, language, producerOfProduct);
			
			if(!namedGraph) {
				review.setPublishDate(publishDateGen.randomDateInMillis(reviewDate, today.getTimeInMillis()));
			}
			//needed for qualified name
			review.setPublisher(person.getPublisher());
			
			bundle.add(review);
			reviewNr++;
		}
	}
	
	/*
	 * Process the program parameters typed on the command line.
	 */
	private static void processProgramParameters(String[] args) {
		int i=0;
		while(i<args.length) {
			try {
				if(args[i].equals("-s")) {
					serializerType = args[i++ + 1];
				}
				else if(args[i].equals("-pc")) {
					productCount = Integer.parseInt(args[i++ + 1]);
				}
				else if(args[i].equals("-fc")) {
					forwardChaining = true;
				}
				else if(args[i].equals("-dir")) {
					outputDirectory = args[i++ + 1];
				}
				else if(args[i].equals("-fn")) {
					outputFileName = args[i++ + 1];
				}
				else {
					printUsageInfos();
					System.exit(-1);
				}
				
				i++;
							
			} catch(Exception e) {
				System.err.println("Invalid arguments\n");
				printUsageInfos();
				System.exit(-1);
			}
		}
	}
	
	/*
	 * print command line options
	 */
	public static void printUsageInfos() {
		String output = "Usage: java benchmark.generator.Generator <options>\n\n" +
						"Possible options are:\n" +
						"\t-s <output format>\n" +
						"\t\twhere <output format>: nt (N-Triples), trig (TriG), ttl (Turtle), sql (MySQL dump), virt (Virtuoso SQL dump), xml (XML dump)\n" +
						"\t\tdefault: nt\n" +
						"\t\tNote:\tBy chosing a named graph output format like TriG,\n\t\t\ta named graph model gets generated.\n" +
						"\t-pc <product count>\n" +
						"\t\tdefault: 100\n" +
						"\t-fc\tSwitch on forward chaining which is by default off\n" +
						"\t-dir <output directory>\n" +
						"\t\tThe output directory for the Test Driver data\n" +
						"\t\tdefault: td_data\n" +
						"\t-fn <dataset file name>\n" +
						"\t\tThe file name without the output format suffix\n" +
						"\t\tdefault: dataset\n";
		System.out.print(output);
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		processProgramParameters(args);
		init();
		
		createProductTypeHierarchy();
		createProductFeatures();
		createProducerData();
		createVendorData();
		createRatingSiteData();
		
		serializer.serialize();
		writeTestDriverData();
		
		System.out.println(serializer.triplesGenerated() + " triples generated.");
	}
}