package com.ericsson.eniq.etl.ct;

import java.rmi.Naming;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;

import com.distocraft.dc5000.common.RmiUrlFactory;
import com.distocraft.dc5000.etl.parser.*;
import com.distocraft.dc5000.etl.parser.Parser;
import com.distocraft.dc5000.etl.parser.utils.FlsUtils;
import com.ericsson.eniq.common.ENIQEntityResolver;
import com.ericsson.eniq.enminterworking.EnmInterCommonUtils;
import com.ericsson.eniq.enminterworking.IEnmInterworkingRMI;

/**
 * 
 * <br>
 * <table border="1" width="100%" cellpadding="3" cellspacing="0">
 * <tr bgcolor="#CCCCFF" class="TableHeasingColor">
 * <td colspan="4"><font size="+2"><b>Parameter Summary</b></font></td>
 * </tr>
 * <tr>
 * <td><b>Name</b></td>
 * <td><b>Key</b></td>
 * <td><b>Description</b></td>
 * <td><b>Default</b></td>
 * </tr>
 * <tr>
 * <td>VendorID from</td>
 * <td>readVendorIDFrom</td>
 * <td>Defines where the vendorID is retrieved from <b>data</b> (moid-tag) or
 * from <b>filename</b>. RegExp is used to further define the actual vendorID.
 * Vendor id is added to the outputdata as objectClass. See. VendorID Mask and
 * objectClass</td>
 * <td>data</td>
 * </tr>
 * <tr>
 * <td>VendorID Mask</td>
 * <td>vendorIDMask</td>
 * <td>Defines the RegExp mask that is used to extract the vendorID from either
 * data or filename. See. VendorID from</td>
 * <td>&nbsp;</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>multiline</td>
 * <td>are multiple sequences (seq) handled as separate lines (multiline = true)
 * or single line with items comma delimited (multiline = false).</td>
 * <td>false</td>
 * </tr>
 * </table>
 * <br>
 * <br>
 * <table border="1" width="100%" cellpadding="3" cellspacing="0">
 * <tr bgcolor="#CCCCFF" class="TableHeasingColor">
 * <td colspan="2"><font size="+2"><b>Added DataColumns</b></font></td>
 * </tr>
 * <tr>
 * <td><b>Column name</b></td>
 * <td><b>Description</b></td>
 * </tr>
 * <tr>
 * <td>filename</td>
 * <td>contains the filename of the inputdatafile.</td>
 * </tr>
 * <tr>
 * <td>fdn</td>
 * <td>contains the data from FDN tag.</td>
 * </tr>
 * <tr>
 * <td>seq</td>
 * <td>contains the data from MOID tag.</td>
 * </tr>
 * <tr>
 * <td>PERIOD_DURATION</td>
 * <td>contains the data from GP tag.</td>
 * </tr>
 * <tr>
 * <td>DATETIME_ID</td>
 * <td>contains the data from CBT tag.</td>
 * </tr>
 * <tr>
 * <td>objectClass</td>
 * <td>contains the same data as in vendorID (see. readVendorIDFrom)</td>
 * </tr>
 * <tr>
 * <td>DC_SUSPECTFLAG</td>
 * <td>contains the sf -tag.</td>
 * </tr>
 * <tr>
 * <td>DIRNAME</td>
 * <td>Conatins full path to the inputdatafile.</td>
 * </tr>
 * <tr>
 * <td>JVM_TIMEZONE</td>
 * <td>contains the JVM timezone (example. +0200)</td>
 * </tr>
 * <td>&nbsp;</td>
 * <td>&nbsp;</td>
 * </tr>
 * </table>
 * <br>
 * <br>
 * 
 * @author savinen <br>
 *         <br>
 * 
 */
public class CTParser extends DefaultHandler implements Parser {

	// Virtual machine timezone unlikely changes during execution of JVM
	private static final String JVM_TIMEZONE = (new SimpleDateFormat("Z")).format(new Date());

	private Logger log;

	private SourceFile sourceFile;

	private String charValue = "";

	private String fdn;

	private String objectMask;

	private String objectClass;

	private String oldObjectClass;

	private String readVendorIDFrom;

	private String key;

	private String seqKey = "";

	private int seqCount = 0;

	private ArrayList seqContainer = null;

	private MeasurementFile measFile = null;

	private Map measData;

	private boolean multiLine = false;
	private Map<String, String> itemValuesMap = new HashMap<String, String>();

	// ***************** Worker stuff ****************************

	private String techPack;

	private String setType;

	private String setName;

	private int status = 0;

	private Main mainParserObject = null;

	private final static String EMPTY_SUSPECT_FLAG = "";

	private String workerName = "";

	private static String nodeVersion;

	private static boolean flag;

	private static boolean flag1;

	// ******************60K

	private HashMap<String, String> nodefdn;

	private String ne_type = "";

	private boolean ne_typeExist = false;

	private boolean isFlsEnabled = false;

	private String serverRole = null;

	private IEnmInterworkingRMI multiEs;
	
	private Map<String, String> ossIdToHostNameMap;
	private long parseStartTime;
	private long fileSize = 0L;
	private long totalParseTime = 0L;
	private int fileCount = 0;


	@Override
	public void init(final Main main, final String techPack, final String setType, final String setName,
			final String workerName) {
		this.mainParserObject = main;
		this.techPack = techPack;
		this.setType = setType;
		this.setName = setName;
		this.status = 1;
		this.workerName = workerName;

		String logWorkerName = "";
		if (workerName.length() > 0) {
			logWorkerName = "." + workerName;
		}

		log = Logger.getLogger("etl." + techPack + "." + setType + "." + setName + ".parser.XML" + logWorkerName);
		
	}

	@Override
	public int status() {
		return status;
	}

	@Override
	public void run() {

		try {
			this.status = 2;
			SourceFile sf = null;
			parseStartTime = System.currentTimeMillis();


			while ((sf = mainParserObject.nextSourceFile()) != null) {
				fileCount++;
				fileSize += sf.fileSize();

				if (sf.getName().endsWith(".xml")) {
					try {
						mainParserObject.preParse(sf);
						parse(sf, techPack, setType, setName);
						mainParserObject.postParse(sf);
					} catch (Exception e) {
						mainParserObject.errorParse(e, sf);
					} finally {
						mainParserObject.finallyParse(sf);
					}
				}
			}
			totalParseTime = System.currentTimeMillis() - parseStartTime;
			if (totalParseTime != 0) {
				log.info("Parsing Performance :: " + fileCount
						+ " files parsed in " + totalParseTime / 1000
						+ " sec, filesize is " + fileSize / 1000
						+ " Kb and throughput : " + (fileSize / totalParseTime)
						+ " bytes/ms.");
			}
		} catch (Exception e) {
			// Exception catched at top level. No good.
			log.log(Level.WARNING, "Worker parser failed to exception", e);
		} finally {
			this.status = 3;
		}
	}

	// ***************** Worker stuff ****************************

	@Override
	public void parse(final SourceFile sf, final String techPack, final String setType, final String setName)
			throws Exception {
		measData = new HashMap();

		this.sourceFile = sf;

		log.finest("Reading configuration...");

		final XMLReader xmlReader = new org.apache.xerces.parsers.SAXParser();
		xmlReader.setContentHandler(this);
		xmlReader.setErrorHandler(this);

		objectMask = sf.getProperty("vendorIDMask", ".+,(.+)=.+");
		readVendorIDFrom = sf.getProperty("readVendorIDFrom", "data");
		multiLine = "TRUE".equalsIgnoreCase(sf.getProperty("multiline", "false"));

		log.fine("Staring to parse...");

		xmlReader.setEntityResolver(new ENIQEntityResolver(log.getName()));
		xmlReader.parse(new InputSource(sf.getFileInputStream()));

		log.fine("Parse finished.");
	}

	/**
	 * Event handlers
	 */
	@Override
	public void startDocument() {
		log.finest("Start document");
		oldObjectClass = null;
		nodefdn = new HashMap<String, String>();
	}

	@Override
	public void endDocument() throws SAXException {
		try {
			String dir = sourceFile.getProperty("inDir");
			String enmDir=getOSSIdFromInDir(dir);
			String enmAlias = Main.resolveDirVariable(enmDir);
			isFlsEnabled = FlsUtils.isFlsEnabled(enmAlias);
			if (isFlsEnabled) {
				multiEs = (IEnmInterworkingRMI) Naming
						.lookup(RmiUrlFactory.getInstance().getMultiESRmiUrl(EnmInterCommonUtils.getEngineIP()));
				String enmHostName = FlsUtils.getEnmShortHostName(enmAlias);
				try {
					for (Map.Entry<String, String> entry : nodefdn.entrySet()) {
						if ( ( entry.getValue() != null && !entry.getValue().equals("") ) && ( entry.getKey() != null && !entry.getKey().equals("") ) && enmHostName != null){
							log.info("In FLS mode. Adding FDN to the Automatic Node Assignment Blocking Queue "
									+ entry.getValue() + " " + entry.getKey() + " " + enmHostName);
							multiEs.addingToBlockingQueue(entry.getValue(), entry.getKey(), enmHostName);
						}

					}
				} catch (Exception abqE) {
					log.log(Level.WARNING, "Exception occured while adding FDN to Blocking Queue!", abqE);
				}

			}
			else
			{
				log.finest("FLS is not enabled. FDN will not be added to blocking Queue!");
			}
		} catch (Exception e) {
			log.log(Level.WARNING, "Exception occured while checking for FLS", e);
		}
		log.finest("End document");
		measData.clear();
		sourceFile = null;
		itemValuesMap.clear();

		// reset seq also
		seqKey = "";
		seqCount = 0;
		seqContainer = null;

	}

	@Override
	public void startElement(final String uri, final String name, final String qName, final Attributes atts)
			throws SAXException {

		charValue = "";

		if (qName.equals("mo")) {

			// read object class from data
			for (int i = 0; i < atts.getLength(); i++) {
				if (atts.getLocalName(i).equalsIgnoreCase("fdn")) {
					fdn = atts.getValue(i);
					log.log(Level.FINEST, "fdn: " + fdn);
				}
			}

			// where to read objectClass (moid)
			if (readVendorIDFrom.equalsIgnoreCase("file")) {

				// read vendor id from file
				objectClass = parseFileName(sourceFile.getName(), objectMask);

			} else if (readVendorIDFrom.equalsIgnoreCase("data")) {

				// read vendor id from file
				objectClass = parseFileName(fdn, objectMask);

			} else if (!readVendorIDFrom.equalsIgnoreCase("file") && !readVendorIDFrom.equalsIgnoreCase("data") && readVendorIDFrom != null){
				
				// read vendor id from the tag
				objectClass=readVendorIDFrom;
				log.finest("Reading vendor id from the given tag:" + objectClass);
			
			} else {

				// error
				log.warning("readVendorIDFrom property" + readVendorIDFrom + " is not defined");
				throw new SAXException("readVendorIDFrom property" + readVendorIDFrom + " is not defined");
			}
		}
		if (qName.equals("attr")) {

			for (int i = 0; i < atts.getLength(); i++) {
				if (atts.getLocalName(i).equalsIgnoreCase("name")) {
					key = atts.getValue(i);
				}
				
			}
			
		}
		if (qName.equals("seq")) {

			seqKey = key;

			for (int i = 0; i < atts.getLength(); i++) {
				if (atts.getLocalName(i).equalsIgnoreCase("count")) {
					seqCount = Integer.parseInt(atts.getValue(i));
				}
			}

			seqContainer = new ArrayList();

		}
	}

	@Override
	public void endElement(final String uri, final String name, final String qName) throws SAXException {

		if (qName.equals("item")) {
			if (itemValuesMap.get(key) != null) {
				String itemValues = itemValuesMap.get(key);
				itemValues = itemValues.concat(",").concat(charValue);
				itemValuesMap.put(key, itemValues);
			} else {
				itemValuesMap.put(key, charValue);
			}

			if (seqContainer.size() < seqCount) {
				seqContainer.add(charValue);
			}

		} else if (qName.equals("seq")) {

		} else if (qName.equals("attr")) {
			log.log(Level.FINEST, "Key:" + key + " Items Value: " + itemValuesMap.get(key)
					+ " Value from measData.get(key) : " + measData.get(key));
			
			if(objectClass.equalsIgnoreCase(key)){
				
				if (itemValuesMap.get(key) != null) {
					objectClass = parseFileName(itemValuesMap.get(key), objectMask);
				}
				else {
					objectClass = parseFileName(charValue, objectMask);
				}
				
				objectClass=objectClass.trim();
			}

			if (itemValuesMap.get(key) != null) {
				measData.put(key, itemValuesMap.get(key));
			} else {

				log.log(Level.FINEST, "Key: " + key + " Value: " + charValue);
				measData.put(key, charValue);
				
			}

			// Strange case of HSS
			String item = (String) measData.get("managedElementType");
			if (item != null) {
				ne_typeExist = true;
				ne_type = item;
				if (item.trim().equalsIgnoreCase("HSS-FE".trim()) && !flag) {
					nodeVersion = (String) measData.get("nodeVersion");
				}
			}
			if (nodeVersion != null) {
				flag = true;
			}
			item = (String) measData.get("Hss_feFunctionId");
			if (item != null && !flag1) {
				measData.put("nodeVersion", nodeVersion);
				log.log(Level.FINEST, "Key: nodeVersion" + " Value: " + nodeVersion);
				flag1 = true;
			}

		} else if (qName.equals("mo")) {

			if (objectClass != null) {

				try {

					if (!objectClass.equals(oldObjectClass)) {

						log.log(Level.FINEST, "New objectClass found: " + objectClass);

						oldObjectClass = objectClass;
						// close old meas file
						if (measFile != null) {
							measFile.close();
						}

						measFile = Main.createMeasurementFile(sourceFile, objectClass, techPack, setType, setName,
								this.workerName, this.log);
						

					} else {
						log.log(Level.FINEST, "Old objectClass, no need to create new measFile " + oldObjectClass);
					}

					// no sequenses just add once
					if (seqContainer == null || seqContainer.size() == 0) {

						measFile.addData("Filename", sourceFile.getName());
						measFile.addData("DC_SUSPECTFLAG", EMPTY_SUSPECT_FLAG);
						measFile.addData("DIRNAME", sourceFile.getDir());
						measFile.addData("objectClass", objectClass);
						measFile.addData("fdn", fdn);
						measFile.addData("JVM_TIMEZONE", JVM_TIMEZONE);
						measFile.addData(measData);
						measFile.saveData();

					} else {

						if (!multiLine) {

							// there is sequnce but we want only one datarow.
							final StringBuffer tmp = new StringBuffer();
							for (int i = 0; i < seqContainer.size(); i++) {
								if (i > 0) {
									tmp.append(",");
								}
								tmp.append((String) seqContainer.get(i));
							}

							measFile.addData(measData);
							measFile.addData(seqKey, tmp.toString());
							measFile.addData("Filename", sourceFile.getName());
							measFile.addData("DC_SUSPECTFLAG", EMPTY_SUSPECT_FLAG);
							measFile.addData("DIRNAME", sourceFile.getDir());
							measFile.addData("objectClass", objectClass);
							measFile.addData("fdn", fdn);
							measFile.addData("JVM_TIMEZONE", JVM_TIMEZONE);
							measFile.saveData();

						} else {

							// there is sequence and we want multiple datarows
							// -> clone data.
							for (int i = 0; i < seqContainer.size(); i++) {

								measFile.addData(measData);
								measFile.addData(seqKey, (String) seqContainer.get(i));
								measFile.addData("Filename", sourceFile.getName());
								measFile.addData("DC_SUSPECTFLAG", EMPTY_SUSPECT_FLAG);
								measFile.addData("DIRNAME", sourceFile.getDir());
								measFile.addData("objectClass", objectClass);
								measFile.addData("fdn", fdn);
								measFile.addData("JVM_TIMEZONE", JVM_TIMEZONE);
								measFile.saveData();
							}
						}
						
						seqContainer.clear();
					}
					if (ne_typeExist) {
						log.finest("inside mo " + fdn + "   " + ne_type);
						nodefdn.put(fdn, ne_type);
						ne_typeExist = false;
                    }
					
					measData.clear();

				} catch (Exception e) {
					log.log(Level.FINE, "Error in writing measurement file", e);
				}

			}

		}
	}

	@Override
	public void characters(final char ch[], final int start, final int length) {
		final StringBuffer charBuffer = new StringBuffer(length);
		for (int i = start; i < start + length; i++) {
			// If no control char
			if (ch[i] != '\\' && ch[i] != '\n' && ch[i] != '\r' && ch[i] != '\t') {
				charBuffer.append(ch[i]);
			}
		}
		charValue += charBuffer;
	}

	/**
	 * Extracts a substring from given string based on given regExp
	 * 
	 */
	public String parseFileName(final String str, final String regExp) {

		final Pattern pattern = Pattern.compile(regExp);
		final Matcher matcher = pattern.matcher(str);

		if (matcher.matches()) {
			final String result = matcher.group(1);
			log.finest(" regExp (" + regExp + ") found from " + str + "  :" + result);
			return result;
		} else {
			log.warning("String " + str + " doesn't match defined regExp " + regExp);
		}

		return "";
	}
	
	/**
	 * Lookups variables from filename
	 * 
	 * @param name
	 *            of the input file
	 * @param pattern
	 *            the pattern to lookup from the filename
	 * @return result returns the group(1) of result, or null
	 */
	private String transformFileVariables(String filename, String pattern) {
		String result = null;
		try {
			Pattern p = Pattern.compile(pattern);
			Matcher m = p.matcher(filename);
			if (m.matches()) {
				result = m.group(1);
			}
		} catch (PatternSyntaxException e) {
			log.log(Level.SEVERE, "Error performing transformFileVariables for CT Parser.", e);
		}
		return result;
	}
	
	/**
	 * Lookups variables from filename
	 * 
	 * @param name
	 *            of the input file
	 * @return result returns OSSId, or null
	 */
	private String getOSSIdFromInDir(String filename) {
		String result = null;
		try {
			if(filename.contains("/"))
			{
				result=filename.split("/")[1];
			}
			else
			{
				log.warning("InDir of the source file is not proper");
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error performing getOSSIdFromInDir ", e);
		}
		return result;
	}

	
}