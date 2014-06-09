/**
 * 
 */

package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.SortedMap;

/**
 * @author Pratik
 */
public class SingleTableIndexEvaluator implements Runnable {

    private static LinkedHashMap<Integer, List<Tuple>> outstream;
    private static PlainSelect select;
    private Expression where;
    public static HashMap<String, TableIndexInfo> indices;
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    private static HashMap<String, TableExpInfo> tabinfo;
    private final List<Expression> where_exp_list;
    String tableName;
    List<Expression> where_exp;
    boolean isSingleTable;
    public HashMap<Integer, List<String>> tableOutput;
    String colName;

    public SingleTableIndexEvaluator(List<Table> tables, PlainSelect sel) {
        select = sel;
        indices = new HashMap<String, TableIndexInfo>();
        outstream = new LinkedHashMap<Integer, List<Tuple>>();
        tabinfo = new HashMap<String, TableExpInfo>();
        where_exp_list = new ArrayList<Expression>();

        tableName = tables.get(0).getWholeTableName().toLowerCase();

        ExpressionEvaluator e = new ExpressionEvaluator();
        where = select.getWhere();
        Expression exp = where;
        exp.accept(e);
        where_exp = e.getExpList();

        if (where_exp.size() == 0) {
            where_exp.add(where);
        }
        List<Expression> explist = new ArrayList<Expression>();

        for (Expression w : where_exp) {
            if (!w.toString().contains(" AND ")) {
                explist.add(w);
            }
        }

        where_exp = explist;
        isSingleTable = true;
        new Indexer().createIndices();
        indices = EvaluateStatement.indices;

    }

    public SingleTableIndexEvaluator(String tableName, String colName,
            List<Expression> where_exp) {

        if (EvaluateStatement.aliasMap.containsKey(tableName)) {
            tableName = EvaluateStatement.aliasMap.get(tableName);
        }
        this.tableName = tableName;
        indices = new HashMap<String, TableIndexInfo>();
        outstream = new LinkedHashMap<Integer, List<Tuple>>();
        tabinfo = new HashMap<String, TableExpInfo>();
        where_exp_list = new ArrayList<Expression>();
        this.where_exp = where_exp;
        isSingleTable = false;
        tableOutput = new HashMap<Integer, List<String>>();
        this.colName = colName;
        indices = EvaluateStatement.indices;
    }

    public LinkedHashMap<Integer, List<Tuple>> evaluate() {

        TableIndexInfo idInfo = indices.get(tableName);

        if (idInfo.primaryStringIndex != null) {
            if (EvaluateStatement.tables.size() < 3 || EvaluateStatement.isLimit) {
                return evaluateDateString(idInfo);
            } else {
                return evaluateDateString2(idInfo);
            }
        } else if (idInfo.primaryIntegerIndex != null) {
            if (EvaluateStatement.tables.size() < 3) {
                return evaluateDateInteger(idInfo);
            } else {
                return evaluateDateInteger2(idInfo);
            }
        }
        return null;
    }

    private LinkedHashMap<Integer, List<Tuple>> evaluateDateInteger(
            TableIndexInfo idInfo) {

        Date toKey = null;
        Date fromKey = null;
        if (idInfo.secondaryDateIntegerIndex != null) {
            fromKey = idInfo.secondaryDateIntegerIndex.firstKey();
            toKey = idInfo.secondaryDateIntegerIndex.lastKey();
        }

        boolean usedSortedMap = false;
        if (where_exp != null) {
            for (Expression wh : where_exp) {
                boolean isIndex = false;
                LinkedHashSet<String> colnames;
                String col = "";
                ExpressionEvaluator e1 = new ExpressionEvaluator();
                wh.accept(e1);
                colnames = e1.getColNames();
                for (String c : colnames) {
                    if (!c.contains("|")) {
                        col = c;
                    }
                }

                if (col.contains(".")) {
                    col = col.split("\\.")[1];
                }
                for (String s : idInfo.secondaryKeyList) {
                    if (s.equals(col)) {
                        isIndex = true;
                    }
                }

                if (isIndex) {
                    usedSortedMap = true;

                    String condition = wh.toString();
                    try {

                        if (condition.contains("<=")) {
                            String rhs = condition.split("\\<\\=")[1];
                            rhs = rhs.trim();
                            rhs = rhs.replaceAll("DATE\\('", "");
                            rhs = rhs.replaceAll("date\\('", "");
                            rhs = rhs.replaceAll("'\\)", "");

                            Date d = sdf.parse(rhs);

                            for (Date lhs : idInfo.secondaryDateIntegerIndex.keySet()) {
                                if (lhs.compareTo(d) > 0) {
                                    break;
                                } else {
                                    toKey = lhs;
                                }
                            }

                        } else if (condition.contains(">=")) {
                            // long st = System.currentTimeMillis();
                            String rhs = condition.split("\\>\\=")[1];
                            rhs = rhs.trim();
                            rhs = rhs.replaceAll("DATE\\('", "");
                            rhs = rhs.replaceAll("date\\('", "");
                            rhs = rhs.replaceAll("'\\)", "");

                            Date d = sdf.parse(rhs);
                            for (Date lhs : idInfo.secondaryDateIntegerIndex.keySet()) {
                                if (lhs.compareTo(d) >= 0) {
                                    fromKey = lhs;
                                    break;
                                }

                            }

                            // long et = System.currentTimeMillis();
                            // System.out.println("ADD >+: " + (et - st));

                        } else if (condition.contains("<")) {
                            // long st = System.currentTimeMillis();
                            String rhs = condition.split("\\<")[1];
                            rhs = rhs.trim();
                            rhs = rhs.replaceAll("DATE\\('", "");
                            rhs = rhs.replaceAll("date\\('", "");
                            rhs = rhs.replaceAll("'\\)", "");

                            Date d = sdf.parse(rhs);
                            for (Date lhs : idInfo.secondaryDateIntegerIndex.keySet()) {

                                if (lhs.compareTo(d) >= 0) {
                                    break;
                                } else {
                                    toKey = lhs;
                                }
                            }
                            // long et = System.currentTimeMillis();
                            // System.out.println("ADD <: " + (et - st));
                        }

                    } catch (ParseException e2) {
                        e2.printStackTrace();
                    }

                } else {

                    where_exp_list.add(wh);

                }

            }
        }

        getSingleExpressions();
        String expression = null;
        String[] colNames = null;
        int[] cntList = null;
        String[] datatype = null;
        boolean isConditionPresent = false;
        BooleanEvaluator be = null;
        ArrayList<Metadata>[] mlist = null;
        if (tabinfo.containsKey(tableName)) {
            colNames = new String[tabinfo.get(tableName).getCol().size()];
            datatype = new String[tabinfo.get(tableName).getCol().size()];
            tabinfo.get(tableName).setCntList();
            tabinfo.get(tableName).setDatatype();
            expression = tabinfo.get(tableName).getExp();
            int count1 = 0;
            for (String temp : tabinfo.get(tableName).getCol()) {
                colNames[count1++] = temp;
            }
            cntList = tabinfo.get(tableName).getCntList();
            datatype = tabinfo.get(tableName).getDatatype();

            mlist = tabinfo.get(tableName).getAndOrList();
            be = new BooleanEvaluator();
            isConditionPresent = true;
            // System.out.println("METADATA...................");
            // for (Metadata m : mlist[0]) {
            // m.printMetadata();
            // }
            // System.out.println("..............................");
        }

        if (isSingleTable) {

            return singleTableReturnInteger(isConditionPresent, null, be,
                    mlist, idInfo);

        } else {

            int colNo = ConditionEvaluator.getColNumber(colName, tableName);
            if (isConditionPresent) {
                if (usedSortedMap) {
                    // long st = System.currentTimeMillis();
                    for (Date d : idInfo.secondaryDateIntegerIndex.keySet()) {
                        if ((d.equals(fromKey) || d.after(fromKey)) && (d.equals(toKey)
                                || d.before(toKey))) {
                            for (String s : idInfo.secondaryDateIntegerIndex
                                    .getPrimaryValues(d)) {
                                if (be.calculateBoolean(mlist, s)) {
                                    insert(s, colNo);
                                }
                            }
                        }

                        if (d.equals(toKey)) {
                            break;
                        }

                    }

                    // long et = System.currentTimeMillis();
                    // System.out.println("LOOP1: " + (et - st));
                } else {
                    for (Integer i : idInfo.primaryIntegerIndex.keySet()) {
                        String s = idInfo.primaryIntegerIndex.get(i);
                        if (be.calculateBoolean(mlist, s)) {
                            insert(s, colNo);
                        }
                    }

                }
            } else {
                if (usedSortedMap) {
                    // long st = System.currentTimeMillis();
                    for (Date d : idInfo.secondaryDateIntegerIndex.keySet()) {
                        if ((d.equals(fromKey) || d.after(fromKey)) && (d.equals(toKey)
                                || d.before(toKey))) {
                            for (String s : idInfo.secondaryDateIntegerIndex
                                    .getPrimaryValues(d)) {
                                insert(s, colNo);
                            }
                        }

                        if (d.equals(toKey)) {
                            break;
                        }

                    }
                    // long et = System.currentTimeMillis();
                    // System.out.println("LOOP2: " + (et - st));
                } else {

                    for (Integer i : idInfo.primaryIntegerIndex.keySet()) {
                        String s = idInfo.primaryIntegerIndex.get(i);
                        insert(s, colNo);

                    }

                }
            }

        }
        return null;

    }

    private LinkedHashMap<Integer, List<Tuple>> evaluateDateString(
            TableIndexInfo idInfo) {

        Date toKey = null;
        Date fromKey = null;

        if (idInfo.secondaryDateStringIndex != null) {
            fromKey = idInfo.secondaryDateStringIndex.firstKey();
            toKey = idInfo.secondaryDateStringIndex.lastKey();
        }

        boolean usedSortedMap = false;
        if (where_exp != null) {
            for (Expression wh : where_exp) {
                boolean isIndex = false;
                LinkedHashSet<String> colnames;
                String col = "";
                ExpressionEvaluator e1 = new ExpressionEvaluator();
                wh.accept(e1);
                colnames = e1.getColNames();
                for (String c : colnames) {
                    if (!c.contains("|")) {
                        col = c;
                    }
                }

                for (String s : idInfo.secondaryKeyList) {
                    if (s.equals(col)) {
                        isIndex = true;
                    }
                }
                if (isIndex) {
                    usedSortedMap = true;

                    String condition = wh.toString();
                    try {

                        if (condition.contains("<=")) {
                            String rhs = condition.split("\\<\\=")[1];
                            rhs = rhs.trim();
                            rhs = rhs.replaceAll("DATE\\('", "");
                            rhs = rhs.replaceAll("date\\('", "");
                            rhs = rhs.replaceAll("'\\)", "");

                            Date d = sdf.parse(rhs);

                            for (Date lhs : idInfo.secondaryDateStringIndex.keySet()) {
                                if (lhs.compareTo(d) > 0) {
                                    break;
                                } else {
                                    toKey = lhs;
                                }
                            }

                        } else if (condition.contains(">=")) {
                            String rhs = condition.split("\\>\\=")[1];
                            rhs = rhs.trim();
                            rhs = rhs.replaceAll("DATE\\('", "");
                            rhs = rhs.replaceAll("date\\('", "");
                            rhs = rhs.replaceAll("'\\)", "");

                            Date d = sdf.parse(rhs);
                            for (Date lhs : idInfo.secondaryDateStringIndex.keySet()) {
                                if (lhs.compareTo(d) >= 0) {
                                    fromKey = lhs;
                                    break;
                                }

                            }

                        } else if (condition.contains("<")) {
                            String rhs = condition.split("\\<")[1];
                            rhs = rhs.trim();
                            rhs = rhs.replaceAll("DATE\\('", "");
                            rhs = rhs.replaceAll("date\\('", "");
                            rhs = rhs.replaceAll("'\\)", "");

                            Date d = sdf.parse(rhs);
                            for (Date lhs : idInfo.secondaryDateStringIndex.keySet()) {

                                if (lhs.compareTo(d) >= 0) {
                                    break;
                                } else {
                                    toKey = lhs;
                                }
                            }

                        }

                    } catch (ParseException e2) {
                        e2.printStackTrace();
                    }

                } else {

                    where_exp_list.add(wh);

                }

            }
        }

        getSingleExpressions();
        String expression = null;
        String[] colNames = null;
        int[] cntList = null;
        String[] datatype = null;
        boolean isConditionPresent = false;
        BooleanEvaluator be = null;
        ArrayList<Metadata>[] mlist = null;
        if (tabinfo.containsKey(tableName)) {
            colNames = new String[tabinfo.get(tableName).getCol().size()];
            datatype = new String[tabinfo.get(tableName).getCol().size()];
            tabinfo.get(tableName).setCntList();
            tabinfo.get(tableName).setDatatype();
            expression = tabinfo.get(tableName).getExp();
            int count1 = 0;
            for (String temp : tabinfo.get(tableName).getCol()) {
                colNames[count1++] = temp;
            }
            cntList = tabinfo.get(tableName).getCntList();
            datatype = tabinfo.get(tableName).getDatatype();

            mlist = tabinfo.get(tableName).getAndOrList();
            be = new BooleanEvaluator();
            isConditionPresent = true;

            // for (Metadata m : mlist[1]) {
            // m.printMetadata();
            // }
        }

        if (isSingleTable) {

            return singleTableReturnString(isConditionPresent, null, be,
                    mlist, idInfo);

        } else {

            int colNo = ConditionEvaluator.getColNumber(colName, tableName);
            if (isConditionPresent) {
                if (usedSortedMap) {
                    for (Date d : idInfo.secondaryDateStringIndex.keySet()) {
                        if ((d.equals(fromKey) || d.after(fromKey)) && (d.equals(toKey)
                                || d.before(toKey))) {
                            for (String s : idInfo.secondaryDateStringIndex
                                    .getPrimaryValues(d)) {
                                if (be.calculateBoolean(mlist, s)) {
                                    insert(s, colNo);
                                }
                            }

                        }

                        if (d.equals(toKey)) {
                            break;
                        }
                    }
                } else {

                    for (String i : idInfo.primaryStringIndex.keySet()) {
                        String s = idInfo.primaryStringIndex.get(i);
                        // long st = System.nanoTime();
                        if (be.calculateBoolean(mlist, s)) {

                            insert(s, colNo);

                        }
                        // long et = System.nanoTime();
                        // System.out.println("LINEITEM LOOP: " + (et - st));
                    }

                }

            } else {
                if (usedSortedMap) {
                    for (Date d : idInfo.secondaryDateStringIndex.keySet()) {
                        if ((d.equals(fromKey) || d.after(fromKey)) && (d.equals(toKey)
                                || d.before(toKey))) {
                            for (String s : idInfo.secondaryDateStringIndex
                                    .getPrimaryValues(d)) {
                                insert(s, colNo);
                            }
                        }

                        if (d.equals(toKey)) {
                            break;
                        }
                    }
                } else {

                    for (String i : idInfo.primaryStringIndex.keySet()) {
                        String s = idInfo.primaryStringIndex.get(i);
                        insert(s, colNo);

                    }

                }
            }

        }
        return null;

    }

    private LinkedHashMap<Integer, List<Tuple>> evaluateDateInteger2(
            TableIndexInfo idInfo) {

        // long st = System.currentTimeMillis();
        Date toKey = null, fromKey = null;
        int colNo = ConditionEvaluator.getColNumber(colName, tableName);

        if (where_exp != null) {
            for (Expression wh : where_exp) {
                String condition = wh.toString();
                try {

                    if (condition.contains("<=")) {
                        String rhs = condition.split("\\<\\=")[1];
                        rhs = rhs.trim();
                        rhs = rhs.replaceAll("DATE\\('", "");
                        rhs = rhs.replaceAll("date\\('", "");
                        rhs = rhs.replaceAll("'\\)", "");

                        Date d = sdf.parse(rhs);
                        toKey = new Date(d.getTime() + 24 * 60 * 60 * 1000);

                    } else if (condition.contains(">=")) {
                        String rhs = condition.split("\\>\\=")[1];
                        rhs = rhs.trim();
                        rhs = rhs.replaceAll("DATE\\('", "");
                        rhs = rhs.replaceAll("date\\('", "");
                        rhs = rhs.replaceAll("'\\)", "");

                        Date d = sdf.parse(rhs);
                        fromKey = d;
                    } else if (condition.contains("<")) {
                        String rhs = condition.split("\\<")[1];
                        rhs = rhs.trim();
                        rhs = rhs.replaceAll("DATE\\('", "");
                        rhs = rhs.replaceAll("date\\('", "");
                        rhs = rhs.replaceAll("'\\)", "");

                        Date d = sdf.parse(rhs);
                        toKey = d;
                    }

                } catch (ParseException e2) {
                    e2.printStackTrace();
                }

            }

            SortedMap<Date, Iterable<Integer>> temp = idInfo.secondaryDateIntegerIndex.subMap(
                    fromKey,
                    toKey);

            for (Date d : temp.keySet()) {
                for (String s : idInfo.secondaryDateIntegerIndex
                        .getPrimaryValues(d)) {
                    insert(s, colNo);
                }
            }
        } else {
            for (int i : idInfo.primaryIntegerIndex.keySet()) {
                String s = idInfo.primaryIntegerIndex.get(i);
                insert(s, colNo);
            }
        }
        // long et = System.currentTimeMillis();
        // System.out.println(tableName + " " + (et - st));

        return null;

    }

    private LinkedHashMap<Integer, List<Tuple>> evaluateDateString2(
            TableIndexInfo idInfo) {
        // long st = System.currentTimeMillis();

        Date toKey = null, fromKey = null;
        int colNo = ConditionEvaluator.getColNumber(colName, tableName);

        if (where_exp != null) {
            for (Expression wh : where_exp) {
                String condition = wh.toString();
                try {

                    if (condition.contains("<=")) {
                        String rhs = condition.split("\\<\\=")[1];
                        rhs = rhs.trim();
                        rhs = rhs.replaceAll("DATE\\('", "");
                        rhs = rhs.replaceAll("date\\('", "");
                        rhs = rhs.replaceAll("'\\)", "");

                        Date d = sdf.parse(rhs);
                        toKey = new Date(d.getTime() + 24 * 60 * 60 * 1000);

                    } else if (condition.contains(">=")) {
                        String rhs = condition.split("\\>\\=")[1];
                        rhs = rhs.trim();
                        rhs = rhs.replaceAll("DATE\\('", "");
                        rhs = rhs.replaceAll("date\\('", "");
                        rhs = rhs.replaceAll("'\\)", "");

                        Date d = sdf.parse(rhs);
                        fromKey = d;
                    } else if (condition.contains("<")) {
                        String rhs = condition.split("\\<")[1];
                        rhs = rhs.trim();
                        rhs = rhs.replaceAll("DATE\\('", "");
                        rhs = rhs.replaceAll("date\\('", "");
                        rhs = rhs.replaceAll("'\\)", "");

                        Date d = sdf.parse(rhs);
                        toKey = d;
                    }

                } catch (ParseException e2) {
                    e2.printStackTrace();
                }

            }

            SortedMap<Date, Iterable<String>> temp = idInfo.secondaryDateStringIndex.subMap(
                    fromKey,
                    toKey);

            for (Date d : temp.keySet()) {
                for (String s : idInfo.secondaryDateStringIndex
                        .getPrimaryValues(d)) {
                    insert(s, colNo);
                }
            }
        } else {
            for (String i : idInfo.primaryStringIndex.keySet()) {
                String s = idInfo.primaryStringIndex.get(i);
                insert(s, colNo);
            }
        }
        // long et = System.currentTimeMillis();
        // System.out.println(tableName + " " + (et - st));
        return null;

    }

    /**
     * @param s
     * @param colNo
     */
    private void insert(String s, int colNo) {
        int key = Integer.parseInt(s.split("\\|")[colNo]);
        if (tableOutput.containsKey(key)) {
            tableOutput.get(key).add(s);
        } else {
            List<String> temp = new ArrayList<String>();
            temp.add(s);
            tableOutput.put(key, temp);
        }
    }

    /**
     * @param isConditionPresent
     * @param mlist
     * @param be
     * @param sortedMap
     * @param idInfo
     * @return
     */
    private LinkedHashMap<Integer, List<Tuple>> singleTableReturnString(
            boolean isConditionPresent, List<Date> sortedMap,
            BooleanEvaluator be, ArrayList<Metadata>[] mlist,
            TableIndexInfo idInfo) {
        int count = 0;
        if (isConditionPresent) {

            for (Date d1 : sortedMap) {
                for (String s : idInfo.secondaryDateStringIndex
                        .getPrimaryValues(d1)) {
                    if (be.calculateBoolean(mlist, s)) {
                        Tuple t = new Tuple(tableName, s);
                        List<Tuple> tlist = new ArrayList<Tuple>();
                        tlist.add(t);
                        outstream.put(count++, tlist);
                    }
                }
            }

        } else {
            for (Date d1 : sortedMap) {
                for (String s : idInfo.secondaryDateStringIndex
                        .getPrimaryValues(d1)) {
                    Tuple t = new Tuple(tableName, s);
                    List<Tuple> tlist = new ArrayList<Tuple>();
                    tlist.add(t);
                    outstream.put(count++, tlist);
                }
            }
        }

        return outstream;

    }

    private LinkedHashMap<Integer, List<Tuple>> singleTableReturnInteger(
            boolean isConditionPresent, List<Date> sortedMap,
            BooleanEvaluator be, ArrayList<Metadata>[] mlist,
            TableIndexInfo idInfo) {
        int count = 0;
        if (isConditionPresent) {

            for (Date d1 : sortedMap) {
                for (String s : idInfo.secondaryDateStringIndex
                        .getPrimaryValues(d1)) {
                    if (be.calculateBoolean(mlist, s)) {
                        Tuple t = new Tuple(tableName, s);
                        List<Tuple> tlist = new ArrayList<Tuple>();
                        tlist.add(t);
                        outstream.put(count++, tlist);
                    }
                }
            }

        } else {
            for (Date d1 : sortedMap) {
                for (String s : idInfo.secondaryDateStringIndex
                        .getPrimaryValues(d1)) {
                    Tuple t = new Tuple(tableName, s);
                    List<Tuple> tlist = new ArrayList<Tuple>();
                    tlist.add(t);
                    outstream.put(count++, tlist);
                }
            }
        }

        return outstream;

    }

    void getSingleExpressions() {

        LinkedHashSet<String> tab_list = new LinkedHashSet<String>();
        LinkedHashSet<String> col_list = new LinkedHashSet<String>();
        LinkedHashSet<String> colnames;
        for (Expression w : where_exp_list) {
            ExpressionEvaluator e1 = new ExpressionEvaluator();
            w.accept(e1);
            colnames = e1.getColNames();

            if (colnames.size() >= 1) {
                String tabname = tableName;
                if (tabinfo.containsKey(tabname)) {
                    TableExpInfo t = tabinfo.get(tabname);
                    HashSet<String> col = t.getCol();
                    for (Object s : col_list.toArray()) {
                        col.add(s.toString());
                    }
                    t.setCol(col);
                    String exp = t.getExp() + " AND " + w.toString();
                    t.setExp(exp);
                    if (w.toString().contains(" OR ")) {
                        t.getAndOrList()[0].add(null);
                        t.getAndOrList()[1] = createORlist(w.toString());

                    } else {
                        Metadata m = new Metadata();
                        m.setCondition(w.toString());
                        t.getAndOrList()[0].add(m);

                    }

                    tabinfo.put(tabname, t);

                } else {
                    ArrayList<Metadata>[] mlist = new ArrayList[2];
                    mlist[0] = new ArrayList<Metadata>();
                    mlist[1] = new ArrayList<Metadata>();
                    TableExpInfo t = new TableExpInfo();
                    HashSet<String> col = new HashSet<String>();
                    for (Object s : col_list.toArray()) {
                        col.add(s.toString());
                    }

                    t.setCol(col);
                    t.setExp(w.toString());

                    if (w.toString().contains(" OR ")) {
                        mlist[0].add(null);
                        mlist[1] = createORlist(w.toString());
                    } else {
                        Metadata m = new Metadata();
                        m.setCondition(w.toString());
                        mlist[0].add(m);

                    }
                    t.setAndOrList(mlist);

                    tabinfo.put(tabname, t);

                }

            }
            tab_list.clear();
            col_list.clear();
        }

    }

    private ArrayList<Metadata> createORlist(String or) {

        ArrayList<Metadata> orList = new ArrayList<Metadata>();
        or = or.replaceAll("\\(", "");
        or = or.replaceAll("\\)", "");

        String[] temp = or.split(" OR ");
        for (String s : temp) {
            Metadata m = new Metadata();
            m.setCondition(s);
            orList.add(m);
        }

        return orList;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        // TODO Auto-generated method stub
        evaluate();
    }

}
