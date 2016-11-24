package com.hadoop.intellipaat;

public class LibsvmConvertor implements ILibsvmConvertor {

	public static String[] HEADERS = new String[] { "accountId", "brand", "campaignId", "inverseTimestamp", "supc", "category", "pagetype", "site",
			"sellerCode", "amount", "publisherRevenue", "pog", "device_id", "email", "user_id", "adType", "url", "cookieId", "trackerId",
			"creativeId", "timestamp", "relevancy_score", "relevancy_category", "ref_tag", "offer_price", "rating", "discount", "sdplus",
			"no_of_rating", "created_time", "normalized_rating", "os", "browser", "city", "state", "country" };

	@Override
	public String convertToLibsvm(String[] words) {
		StringBuilder sb = new StringBuilder();
		try {
			// Return if ad type is not product and price is zero
			if ((words == null || words.length < 24) || (!(words[24].trim().length() > 0 && Float.parseFloat(words[24].trim()) > 0)
					|| !("product".equalsIgnoreCase(words[15].trim())))) {
				return "";
			}

			sb.append("1:" + ((Float) Float.parseFloat(words[0])).intValue()).append(SPACE) // Account
																							// Id
					.append("2:" + hashCode(words[1])).append(SPACE) // Brand Id
					.append("3:" + hashCode(words[5])).append(SPACE); // category

			// Page Type
			byte[] pageTypes = convertPageTypeToBytes(words[6].trim());
			if (pageTypes != null) {
				sb.append("4:" + pageTypes[0]).append(SPACE).append("5:" + pageTypes[1]).append(SPACE).append("6:" + pageTypes[2]).append(SPACE);
			}

			// Site Ids
			byte[] siteTypes = convertSiteIdToBytes(words[7].trim());
			if (siteTypes != null) {
				sb.append("7:" + siteTypes[0]).append(SPACE).append("8:" + siteTypes[1]).append(SPACE).append("9:" + siteTypes[2]).append(SPACE);
			}

			sb.append("10:" + hashCode(words[8].trim())).append(SPACE) // Seller
																		// Code
					.append("11:" + hashCode(words[11].trim())).append(SPACE) // PogId
					.append("12:" + hashCode(words[12].trim())).append(SPACE) // device
																				// Id
					.append("13:" + hashCode(words[13].trim())).append(SPACE); // Email_id

			if (words[20].trim().length() > 0) {
				int[] timeStamp = convertToDay_Month_Year(words[20].trim());
				if (timeStamp != null) {
					sb.append("14:" + timeStamp[0]).append(SPACE).append("15:" + timeStamp[1]).append(SPACE);
				}
			}

			if (words[21].trim().length() > 0) {
				sb.append("16:" + ((Float) Float.parseFloat(words[21].trim())).intValue()).append(SPACE); // Relevant
																											// score
			}

			if (words[22].trim().length() > 0) {
				sb.append("17:" + hashCode(words[22])).append(SPACE); // Relevant
																		// category
			}

			sb.append("18:" + ((Float) Float.parseFloat(words[24].trim())).intValue()).append(SPACE); // price

			if (words[25].trim().length() > 0) {
				sb.append("19:" + ((Float) Float.parseFloat(words[25].trim())).intValue()).append(SPACE); // rating
			}

			if (words[26].trim().length() > 0) {
				sb.append("20:" + ((Float) Float.parseFloat(words[26].trim())).intValue()).append(SPACE); // discount
			}

			// Sd plus
			if (words[27].trim().length() != 0) {
				boolean sdPlus = Boolean.parseBoolean(words[27].trim());
				if (sdPlus) {
					sb.append("21:" + "1").append(SPACE).append("22:" + "0").append(SPACE);
				} else {
					sb.append("21:" + "0").append(SPACE).append("22:" + "1").append(SPACE);
				}
			}

			/*
			 * // OS if (words[31].trim().length() > 0) { sb.append("23:" +
			 * hashCode(words[31].trim())).append(SPACE); }
			 */

			// Browser
			if (words[32].trim().length() > 0) {
				sb.append("24:" + hashCode(words[32].trim())).append(SPACE);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return "";
		}
		return sb.toString();
	}
}
