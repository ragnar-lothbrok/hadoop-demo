package com.hadoop.intellipaat;

import java.beans.PropertyDescriptor;
import java.beans.PropertyEditor;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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

	private static final String[] dummyColumns = { "platform", "pageType", "adType" };

	private static final String[] ignoreColumnsColumns = { "keyUserDeviceId", "itemPogId", "accId", "dpDay", "dpHour", "osVersion", "browserDetails",
			"guid", "widgetId", "trackerId" };

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

	private int addByteArr(byte[] value, Integer index, StringBuilder stringBuilder) {
		if (value != null) {
			for (int i = 0; i < value.length; i++) {
				stringBuilder.append(index + COLON + value[i]);
				stringBuilder.append(SPACE);
				index = index + i;
			}
		}
		return index;
	}

	private static final String COLON = ":";
	private static final String SEPERATOR = " ";

	private String toCsv(ClickData clickData) throws IllegalArgumentException, IllegalAccessException {
		StringBuilder sb = new StringBuilder();
		sb.append((isEmpty(clickData.getClicked()) ? 0 : 1) + "\t");
		Field[] fields = clickData.getClass().getDeclaredFields();
		int index = 0;
		for (int i = 0; i < fields.length - 1; i++) {
			fields[i].setAccessible(true);
			if (fields[i].getType().equals(String.class)) {

				if ("platform".equalsIgnoreCase(fields[i].getName())) {
					addByteArr(convertSiteIdToBytes(fields[i].get(clickData).toString()), index, sb);
					index++;
				} else if ("pageType".equalsIgnoreCase(fields[i].getName())) {
					addByteArr(convertPageTypeToBytes(fields[i].get(clickData).toString()), index, sb);
					index++;
				} else if ("adType".equalsIgnoreCase(fields[i].getName())) {
					addByteArr(convertAdTypeToBytes(fields[i].get(clickData).toString()), index, sb);
					index++;
				} else {
					boolean isIgnored = false;
					for (int k = 0; k < ignoreColumnsColumns.length; k++) {
						if (ignoreColumnsColumns[k].equalsIgnoreCase(fields[i].getName())) {
							isIgnored = true;
							index++;
							break;
						}
					}
					if (!isIgnored) {
						addHashCode(fields[i].get(clickData).toString(), index, sb);
						index++;
					}
				}
			} else if (fields[i].getType().equals(Boolean.class)) {
				sb.append((index++) + COLON + getValue(Boolean.parseBoolean(fields[i].get(clickData).toString())) + SEPERATOR);
			} else {
				addFloat(fields[i].getFloat(clickData), index, sb);
				index++;
			}
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

	private void createTrainFile(String data, String filePath) throws Exception {
		BufferedWriter br = new BufferedWriter(new FileWriter(
				new File("/home/raghunandangupta/Downloads/click_impression_20161115/click_impression_20161115/" + filePath + ".csv")));
		br.write(data);
		br.close();
	}

	public static void main(String[] args) throws Exception {
		new ModifiedLibsvmConvertor().formatData();
		for (Entry<String, Set<String>> entry : map.entrySet()) {
			if (entry.getValue().size() < 10) {
				System.out.println(entry.getKey() + " " + entry.getValue());
			}
		}
	}

	private void formatData() throws Exception {
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
		CSVReader csvReader = new CSVReader(
				new FileReader(new File("/home/raghunandangupta/Downloads/click_impression_20161115/click_impression_20161115/aa")), ',', '"');
		csvReader.readNext();
		List<ClickData> list = csv.parse(setColumMapping(), csvReader);
		System.out.println("Total records " + list.size());
		StringBuilder sb = new StringBuilder();
		for (Object object : list) {
			sb.append(toCsv((ClickData) object) + "\n");
		}

		createTrainFile(sb.toString(), "libsvm");
	}

	@Override
	public String convertToLibsvm(String[] words) {
		// TODO Auto-generated method stub
		return null;
	}

}