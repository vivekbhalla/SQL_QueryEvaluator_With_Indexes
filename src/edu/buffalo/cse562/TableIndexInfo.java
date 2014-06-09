/**
 * 
 */
package edu.buffalo.cse562;

import java.util.Date;
import java.util.List;

import jdbm.PrimaryTreeMap;
import jdbm.SecondaryTreeMap;

/**
 * @author Pratik
 * 
 */
public class TableIndexInfo {

	public PrimaryTreeMap<String, String> primaryStringIndex;
	public PrimaryTreeMap<Integer, String> primaryIntegerIndex;

	public SecondaryTreeMap<String, String, String> secondaryStringStringIndex;
	public SecondaryTreeMap<String, Integer, String> secondaryStringIntegerIndex;

	public SecondaryTreeMap<Integer, String, String> secondaryIntegerStringIndex;
	public SecondaryTreeMap<Integer, Integer, String> secondaryIntegerIntegerIndex;

	public SecondaryTreeMap<Date, String, String> secondaryDateStringIndex;
	public SecondaryTreeMap<Date, Integer, String> secondaryDateIntegerIndex;

	public List<String> primaryKeyList;
	public List<String> secondaryKeyList;

}
