package com.hadoop.intellipaat;

import java.beans.PropertyDescriptor;
import java.beans.PropertyEditor;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.bean.ColumnPositionMappingStrategy;
import au.com.bytecode.opencsv.bean.CsvToBean;
import au.com.bytecode.opencsv.bean.MappingStrategy;

public class ModifiedLibsvmConvertor implements ILibsvmConvertor {

	private static final String[] columns = { "originalPrice", "price", "reviewCount", "position", "trackerId", "platform", "pageType",
			"searchKeyword", "activeProductCategory", "activeSellerCategory", "sellerRatingSdPlus", "sellerRatingNonSdPlus", "supcBrand",
			"supcSdnonsd", "supcCreatedTime", "accId", "adSpaceType", "adType", "amountSpent", "searchCategory", "searchRelevancyScore", "adSpaceId",
			"supcCat", "pageCategory", "keyUserDeviceId", "wiRatingCount", "itemPogId", "wpPercentageOff", "eventKey", "pogId", "displayName",
			"rating", "ratingCount", "sellerCode", "dpDay", "dpHour", "osVersion", "platformType", "browserDetails", "email", "pincode", "guid",
			"widgetId", "clicked" };

	private static final String[] mixColumns = { "pincode" };

	private static final String[] dummyColumns = { "platform", "pageType"};

	private static final String[] ignoreColumnsColumns = { "eventKey", "adSpaceId", "amountSpent", "searchKeyword", "originalPrice",
			"supcCreatedTime", "keyUserDeviceId", "pogId", "accId", "dpDay", "dpHour", "osVersion", "browserDetails", "guid", "widgetId", "trackerId",
			"adSpaceType", "displayName", "sellerRatingNonSdPlus", "email","adType" };

	public static MappingStrategy<ClickData> setColumMapping() {
		ColumnPositionMappingStrategy<ClickData> strategy = new ColumnPositionMappingStrategy<ClickData>();
		strategy.setType(ClickData.class);
		strategy.setColumnMapping(columns);
		return strategy;
	}

	static Map<String, Set<String>> map = new HashMap<String, Set<String>>();

	private void addFloat(Float value, Integer index, StringBuilder stringBuilder) {
		if (!(value == null || value == 0)) {
			stringBuilder.append(index + COLON + value);
			stringBuilder.append(SPACE);
		}
	}

	private void addHashCode(String value, Integer index, StringBuilder stringBuilder) {
		if (!StringUtils.isEmpty(value)) {
			stringBuilder.append(index + COLON + hashCode(value));
			stringBuilder.append(SPACE);
		}
	}

	public boolean isEmpty(String str) {
		return str == null || str.length() == 0 || "null".equalsIgnoreCase(str);
	}

	static List<String> parameters = new ArrayList<String>();

	static {
		parameters.add("slp");
		parameters.add("clp");
		parameters.add("pdp");
		parameters.add("101");
		parameters.add("102");
		parameters.add("103");
		parameters.add("104");
		parameters.add("105");
	}

	private int addByteArr(byte[] value, Integer index, StringBuilder stringBuilder, String fieldName, String Value) {
		if (value != null) {
			for (int i = 0; i < value.length; i++) {
				if (Value != null && parameters.contains(Value.trim().toLowerCase())) {
					stringBuilder.append(index + COLON + value[i]);
					stringBuilder.append(SPACE);
				}
				index++;
			}
		}
		return index;
	}

	private static final String COLON = ":";
	private static final String SEPERATOR = " ";

	static Map<String, String> featureMap = new TreeMap<String, String>();

	private String toCsv(ClickData clickData) throws IllegalArgumentException, IllegalAccessException {
		StringBuilder sb = new StringBuilder();
		try {
			sb.append((isEmpty(clickData.getClicked()) ? 0 : 1) + "\t");
			Field[] fields = clickData.getClass().getDeclaredFields();
			int index = 1;
			for (int i = 0; i < fields.length - 1; i++) {
				fields[i].setAccessible(true);
				if ("price".equalsIgnoreCase(fields[i].getName())) {
					fields[i].setAccessible(true);
					fields[i].setFloat(clickData, ((Float) (fields[i].getFloat(clickData) / 500)).intValue() + 1);
				}
				if (fields[i].getType().equals(String.class)) {
					if ("platform".equalsIgnoreCase(fields[i].getName())) {
						int temp = index, k = 0;
						index = addByteArr(convertSiteIdToBytes(fields[i].get(clickData).toString()), index, sb, fields[i].getName(),
								fields[i].get(clickData).toString());
						while (temp < index) {
							featureMap.put("f" + temp, platformTypes[k++]);
							temp++;
						}
					} else if ("pageType".equalsIgnoreCase(fields[i].getName())) {
						int temp = index, k = 0;
						index = addByteArr(convertPageTypeToBytes(fields[i].get(clickData).toString()), index, sb, fields[i].getName(),
								fields[i].get(clickData).toString());
						while (temp < index) {
							featureMap.put("f" + temp, pageTypes[k++]);
							temp++;
						}
					} else if ("adType".equalsIgnoreCase(fields[i].getName())) {
						int temp = index, k = 0;
						index = addByteArr(convertAdTypeToBytes(fields[i].get(clickData).toString()), index, sb, fields[i].getName(),
								fields[i].get(clickData).toString());
						while (temp < index) {
							featureMap.put("f" + temp, adTypes[k++]);
							temp++;
						}
					} else {
						boolean isIgnored = false;
						for (int k = 0; k < ignoreColumnsColumns.length; k++) {
							if (ignoreColumnsColumns[k].equalsIgnoreCase(fields[i].getName())) {
								isIgnored = true;
								featureMap.put("f" + index, fields[i].getName());
								index++;
								break;
							}
						}
						if (!isIgnored) {
							addHashCode(fields[i].get(clickData).toString(), index, sb);
							featureMap.put("f" + index, fields[i].getName());
							index++;
						}
					}
				} else if (fields[i].getType().equals(Boolean.class)) {
					sb.append((index++) + COLON + getValue(Boolean.parseBoolean(fields[i].get(clickData).toString())) + SEPERATOR);
					featureMap.put("f" + index, fields[i].getName());
				} else {
					boolean isIgnored = false;
					for (int k = 0; k < ignoreColumnsColumns.length; k++) {
						if (ignoreColumnsColumns[k].equalsIgnoreCase(fields[i].getName())) {
							isIgnored = true;
							featureMap.put("f" + index, fields[i].getName());
							index++;
							break;
						}
					}
					if (!isIgnored) {
						addFloat(fields[i].getFloat(clickData), index, sb);
						featureMap.put("f" + index, fields[i].getName());
						index++;
					}
				}
				for(String column : dummyColumns){
					if(fields[i].getName().equalsIgnoreCase(column)){
						if (fields[i].get(clickData).toString() == null || !parameters.contains(fields[i].get(clickData).toString().trim().toLowerCase())) {
							return null;
						}
					}
				}
			}
		} catch (Exception exception) {
			System.out.println("Exception occured " + clickData + exception);
		}
		return sb.toString();
	}

	private Integer getValue(Boolean value) {
		if (value == null) {
			return 0;
		} else if (value) {
			return 1;
		} else {
			return 2;
		}
	}

	private void createTrainFile(String outputDir, String data, String filePath) throws Exception {
		BufferedWriter br = new BufferedWriter(new FileWriter(new File(outputDir + "/" + filePath + "-libsvm")));
		br.write(data);
		br.close();
	}

	public static void main(String[] args) throws Exception {

		File folder = new File(args[0]);
		Map<String, List<String>> hashMap = new HashMap<String, List<String>>();
		File[] listOfFiles = folder.listFiles();
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				new ModifiedLibsvmConvertor().formatData(listOfFiles[i].getAbsolutePath(), args[1]);
			}
		}
		System.out.println(featureMap);
	}

	private void formatData(String path, String outputDir) throws Exception {
		CsvToBean<ClickData> csv = new CsvToBean<ClickData>() {
			protected Object convertValue(String value, PropertyDescriptor prop) throws InstantiationException, IllegalAccessException {
				PropertyEditor editor = getPropertyEditor(prop);
				Object obj = value;
				if (null != editor) {
					if ("FloatEditor".equalsIgnoreCase(editor.getClass().getSimpleName())) {
						if (!value.matches("[\\-\\+]?[0-9]*(\\.[0-9]+)?")) {
							value = "0.0";
						} else if ((value != null && value.length() == 0) || "null".equalsIgnoreCase(value))
							value = "0.0";
					} else if ("BooleanEditor".equalsIgnoreCase(editor.getClass().getSimpleName())) {
						if (!("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value))) {
							value = null;
						}
					}
					editor.setAsText(value);
					obj = editor.getValue();
				}
				return obj;
			}
		};
		CSVReader csvReader = new CSVReader(new FileReader(new File(path)), ',', '"');
		csvReader.readNext();
		List<ClickData> list = csv.parse(setColumMapping(), csvReader);
		System.out.println("Total records " + list.size());
		StringBuilder clickLibsvm = new StringBuilder();
		StringBuilder nonClickLibsvm = new StringBuilder();
		int clickCount = 0;
		int nonClickCount = 0;
		for (Object object : list) {
			String value = toCsv((ClickData) object);
			if (!isEmpty(value)) {
				if (value.startsWith("1")) {
					clickLibsvm.append(toCsv((ClickData) object) + "\n");
					clickCount++;
				} else {
					nonClickLibsvm.append(toCsv((ClickData) object) + "\n");
					nonClickCount++;
				}
			}
		}
		System.out.println("Total actual records " + (clickCount + nonClickCount));
		createTrainFile(outputDir, clickLibsvm.toString(), "1-" + Calendar.getInstance().getTimeInMillis() + "-" + clickCount);
		createTrainFile(outputDir, nonClickLibsvm.toString(), "0-" + Calendar.getInstance().getTimeInMillis() + "-" + nonClickCount);
	}

	@Override
	public String convertToLibsvm(String[] words) {
		// TODO Auto-generated method stub
		return null;
	}

}
