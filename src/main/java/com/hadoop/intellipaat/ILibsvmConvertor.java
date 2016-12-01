package com.hadoop.intellipaat;

import java.nio.charset.StandardCharsets;
import java.util.Calendar;

import com.google.common.hash.Hashing;

public interface ILibsvmConvertor {

	public static final String SPACE = " ";
	public static final String COLON = ":";

	public static final byte[] UNKNOWN = { 0, 0, 0 };
	public static final byte[] SLP = { 1, 0, 0 };
	public static final byte[] CLP = { 0, 1, 0 };
	public static final byte[] PDP = { 0, 0, 1 };
	
	public static String pageTypes[] = {"SLP","CLP","PDP","UNKNOWN"};

	public static final byte[] WEB = { 1, 0, 0 };
	public static final byte[] WAP = { 0, 1, 0 };
	public static final byte[] APP = { 0, 0, 1 };
	public static final byte[] GENERIC = { 0, 0, 0 };
	
	public static String platformTypes[] = {"WEB","WAP","APP","GENERIC"};

	public static final byte[] SIMILAR_AD = { 1, 0, 0 };
	public static final byte[] SEARCH_AD = { 0, 1, 0 };
	public static final byte[] RETARGATED_AD = { 0, 0, 1 };
	public static final byte[] OTHER = { 0, 0, 0 };
	
	public static String adTypes[] = {"SIMILAR_AD","SEARCH_AD","RETARGATED_AD","OTHER"};

	default int[] convertToDay_Month_Year(String timeStamp) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(Long.parseLong(timeStamp));
		return new int[] { cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.DAY_OF_WEEK) };
	}

	default byte[] convertAdTypeToBytes(String adType) {
		byte[] value = null;
		switch (adType) {
		case "similar_ad":
			value = SIMILAR_AD;
			break;
		case "retargeted_ad":
			value = RETARGATED_AD;
			break;
		case "search_ad":
			value = SEARCH_AD;
			break;
		default:
			value = UNKNOWN;
		}
		return value;
	}

	default byte[] convertPageTypeToBytes(String siteId) {
		byte[] value = null;
		switch (siteId) {
		case "slp":
			value = SLP;
			break;
		case "clp":
			value = CLP;
			break;
		case "pdp":
			value = PDP;
			break;
		default:
			value = UNKNOWN;
		}
		return value;
	}

	default byte[] convertSiteIdToBytes(String siteId) {
		byte[] value = null;
		switch (siteId) {
		case "101":
			value = WEB;
			break;
		case "102":
			value = WAP;
			break;
		case "103":
		case "104":
		case "105":
			value = APP;
			break;
		default:
			value = GENERIC;
		}
		return value;
	}

	default String hashCode(String value) {
		return Math.abs(Hashing.murmur3_32().hashString(value, StandardCharsets.UTF_8).hashCode()) + "";
	}

	String convertToLibsvm(String[] words);

}
