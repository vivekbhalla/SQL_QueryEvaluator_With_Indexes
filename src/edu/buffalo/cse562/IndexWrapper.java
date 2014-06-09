package edu.buffalo.cse562;

import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import net.sf.jsqlparser.expression.Expression;

public class IndexWrapper {
	private String name = null;
	private HashMap<Integer, List<String>> hashindex = null;
	private TreeMap<String, Tuple> treeindex = null;
	private Expression e = null;
	private List<String> tables = null;
	private String[] columns = null;
	private boolean found = false;
	private String indexOn = null;
	private String iterateOn = null;
	private String findValueOf = null;
	private long size[];
	private List<String> alias = null;
	private int colNumIterateOn;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public HashMap<Integer, List<String>> getHashindex() {
		return hashindex;
	}

	public void setHashindex(HashMap<Integer, List<String>> hashindex) {
		this.hashindex = hashindex;
	}

	public TreeMap<String, Tuple> getTreeindex() {
		return treeindex;
	}

	public void setTreeindex(TreeMap<String, Tuple> treeindex) {
		this.treeindex = treeindex;
	}

	public Expression getE() {
		return e;
	}

	public void setE(Expression e) {
		this.e = e;
	}

	public List<String> getTables() {
		return tables;
	}

	public void setTables(List<String> tables) {
		this.tables = tables;
	}

	public boolean isFound() {
		return found;
	}

	public void setFound(boolean found) {
		this.found = found;
	}

	public String[] getColumns() {
		return columns;
	}

	public void setColumns(String[] columns) {
		this.columns = columns;
	}

	public String getIndexOn() {
		return indexOn;
	}

	public void setIndexOn(String indexOn) {
		this.indexOn = indexOn;
	}

	public String getFindValueOf() {
		return findValueOf;
	}

	public void setFindValueOf(String findValueOf) {
		this.findValueOf = findValueOf;
	}

	public long[] getSize() {
		return size;
	}

	public void setSize(long size[]) {
		this.size = size;
	}

	public void print() {
		System.out.println("Find value of: " + getFindValueOf());
		System.out.println("index on: " + getIndexOn());
		System.out.println("get name: " + getName());

		System.out.println("columns: " + getColumns()[0] + " "
				+ getColumns()[1]);
		System.out.println("Expression: " + getE());
		System.out.println("tables : " + getTables());
		System.out.println("Iterate On: " + getIterateOn());
		System.out.println("alias list: " + alias);

	}

	public String getIterateOn() {
		return iterateOn;
	}

	public void setIterateOn(String iterateOn) {
		this.iterateOn = iterateOn;
		String tableName = iterateOn.split("\\.")[0];
		String colName = iterateOn.split("\\.")[1];

		colNumIterateOn = ConditionEvaluator.getColNumber(colName, tableName);
	}

	public List<String> getAlias() {
		return alias;
	}

	public void setAlias(List<String> alias) {
		this.alias = alias;
	}

	public int getColNumIterateOn() {
		return colNumIterateOn;
	}

	public void setColNumIterateOn(int colNumIterateOn) {
		this.colNumIterateOn = colNumIterateOn;
	}
}
