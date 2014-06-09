/**
 * 
 */

package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * @author Pratik
 */
public class MultipleTableIndexEvaluator {

    private static LinkedHashMap<Integer, List<Tuple>> outstream;
    private static PlainSelect select;
    private Expression where;
    public static HashMap<String, TableIndexInfo> indices;
    private static HashMap<String, List<Expression>> tabinfo;
    private List<Expression> where_exp;
    List<IndexWrapper> ilist;
    HashMap<String, HashMap<Integer, List<String>>> maps;
    public static List<String> tableOrder;
    private int globalkey;

    public MultipleTableIndexEvaluator(PlainSelect sel, List<IndexWrapper> ilist) {
        select = sel;
        indices = new HashMap<String, TableIndexInfo>();
        outstream = new LinkedHashMap<Integer, List<Tuple>>();
        tabinfo = new HashMap<String, List<Expression>>();
        where_exp = new ArrayList<Expression>();
        this.ilist = ilist;
        maps = new HashMap<String, HashMap<Integer, List<String>>>();
        globalkey = 0;
        tableOrder = new ArrayList<String>();

    }

    public LinkedHashMap<Integer, List<Tuple>> evaluate() {

        // long startTime = System.currentTimeMillis();

        new Indexer().createIndices();

        // long endTime = System.currentTimeMillis();
        // long totalTime = endTime - startTime;
        // System.out.println("Time After Index: " + totalTime);
        //
        // startTime = System.currentTimeMillis();

        getTableExp();
        int count = 0;
        Thread[] threads = new Thread[ilist.size() + 1];
        SingleTableIndexEvaluator[] st = new SingleTableIndexEvaluator[ilist
                .size() + 1];
        String[] tabs = new String[ilist.size() + 1];
        int tc = 0;
        for (IndexWrapper iw : ilist) {

            if (count == 0) {

                String table2 = iw.getIterateOn().split("\\.")[0];

                String col2 = iw.getIterateOn().split("\\.")[1];
                SingleTableIndexEvaluator st1 = new SingleTableIndexEvaluator(
                        table2, col2, tabinfo.get(table2));
                st[tc] = st1;
                tabs[tc] = table2;
                Thread t = new Thread(st1);
                threads[tc] = t;
                t.start();
                tc++;
                // maps.put(table2, SingleTableIndexEvaluator.tableOutput);

            }

            String tableName = iw.getIndexOn().split("\\.")[0];

            String colName = iw.getIndexOn().split("\\.")[1];
            SingleTableIndexEvaluator st1 = new SingleTableIndexEvaluator(
                    tableName, colName, tabinfo.get(tableName));
            st[tc] = st1;
            tabs[tc] = tableName;
            Thread t = new Thread(st1);
            threads[tc] = t;
            t.start();
            tc++;
            // maps.put(tableName, SingleTableIndexEvaluator.tableOutput);
            count++;
        }

        int tc2 = 0;
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            maps.put(tabs[tc2], st[tc2].tableOutput);
            tc2++;
        }

        int c = 0;
        for (IndexWrapper iw : ilist) {

            if (c == 0) {
                tableOrder.add(iw.getIterateOn().split("\\.")[0]);
            }
            tableOrder.add(iw.getIndexOn().split("\\.")[0]);
            c++;
        }

        // endTime = System.currentTimeMillis();
        // totalTime = endTime - startTime;
        // System.out.println("Time After Push Projection: " + totalTime);

        // startTime = System.currentTimeMillis();

        computeHash(null, ilist.size());

        // endTime = System.currentTimeMillis();
        // totalTime = endTime - startTime;
        // System.out.println("Time After join: " + totalTime);

        return outstream;
    }

    // public LinkedHashMap<Integer, List<Tuple>> evaluate() {
    //
    // long startTime = System.currentTimeMillis();
    // new Indexer().createIndices();
    // long endTime = System.currentTimeMillis();
    // long totalTime = endTime - startTime;
    // System.out.println("Time After Index: " + totalTime);
    // startTime = System.currentTimeMillis();
    //
    // getTableExp();
    // int count = 0;
    // for (IndexWrapper iw : ilist) {
    //
    // if (count == 0) {
    //
    // String table2 = iw.getIterateOn().split("\\.")[0];
    //
    // String col2 = iw.getIterateOn().split("\\.")[1];
    // new SingleTableIndexEvaluator(table2, col2, tabinfo.get(table2))
    // .evaluate();
    //
    // maps.put(table2, SingleTableIndexEvaluator.tableOutput);
    //
    // }
    // String tableName = iw.getIndexOn().split("\\.")[0];
    //
    // String colName = iw.getIndexOn().split("\\.")[1];
    // new SingleTableIndexEvaluator(tableName, colName,
    // tabinfo.get(tableName)).evaluate();
    //
    // maps.put(tableName, SingleTableIndexEvaluator.tableOutput);
    // count++;
    // }
    //
    // int c = 0;
    // for (IndexWrapper iw : ilist) {
    //
    // if (c == 0) {
    // tableOrder.add(iw.getIterateOn().split("\\.")[0]);
    // }
    // tableOrder.add(iw.getIndexOn().split("\\.")[0]);
    // c++;
    // }
    //
    // endTime = System.currentTimeMillis();
    // totalTime = endTime - startTime;
    // System.out.println("Time After Push Projection: " + totalTime);
    //
    // startTime = System.currentTimeMillis();
    // computeHash(null, ilist.size());
    // endTime = System.currentTimeMillis();
    // totalTime = endTime - startTime;
    // System.out.println("Time After join: " + totalTime);
    // return outstream;
    // }

    public void computeHash(List<String> updated, int count) {

        if (count > 0 || count == ilist.size()) {
            // first case-iterate
            if (count == ilist.size()) {
                String readTab = ilist.get(0).getIterateOn().split("\\.")[0];
                String idxTab = ilist.get(0).getIndexOn().split("\\.")[0];
                HashMap<Integer, List<String>> iterate = maps.get(readTab);
                HashMap<Integer, List<String>> lookup = maps.get(idxTab);

                for (Integer i : iterate.keySet()) {
                    List<String> temp1 = iterate.get(i);
                    List<String> temp2 = lookup.get(i);
                    if (temp2 != null) {
                        for (String s1 : temp1) {

                            for (String s2 : temp2) {

                                List<String> toSend = new ArrayList<String>();
                                toSend.add(s1);
                                toSend.add(s2);
                                count = ilist.size();
                                count--;
                                computeHash(toSend, count);
                            }
                        }
                    }

                }
            } else {

                int get = ilist.size() - count;
                // String tableName =
                // ilist.get(get).getIterateOn().split("\\.")[0];
                // String colName =
                // ilist.get(get).getIterateOn().split("\\.")[1];
                // int colNum = ConditionEvaluator
                // .getColNumber(colName, tableName);
                int colNum = ilist.get(get).getColNumIterateOn();

                int key = Integer.parseInt(updated.get(updated.size() - 1)
                        .split("\\|")[colNum]);

                String idxtable = ilist.get(get).getIndexOn().split("\\.")[0];
                HashMap<Integer, List<String>> lookup = maps.get(idxtable);

                List<String> temp3 = lookup.get(key);

                if (temp3 != null) {
                    for (String s : temp3) {
                        List<String> temp = new ArrayList<String>();
                        temp.addAll(updated);
                        temp.add(s);
                        int tempcount = count;
                        tempcount--;
                        computeHash(temp, tempcount);

                    }
                }
            }

        } else {
            // create tuple and add to outstream
            int tabNum = 0;
            List<Tuple> tlist = new ArrayList<Tuple>();
            // System.out.println(updated);
            for (String s : updated) {
                Tuple t = new Tuple(tableOrder.get(tabNum++), s);
                tlist.add(t);
            }
            outstream.put(globalkey++, tlist);
        }

    }

    public void getTableExp() {

        ExpressionEvaluator e = new ExpressionEvaluator();
        where = select.getWhere();
        Expression exp = where;
        exp.accept(e);
        where_exp = e.getExpList();
        if (where_exp.size() == 0) {
            where_exp.add(where);
        }

        LinkedHashSet<String> tab_list = new LinkedHashSet<String>();
        LinkedHashSet<String> col_list = new LinkedHashSet<String>();
        LinkedHashSet<String> colnames;

        for (Expression w : where_exp) {

            ExpressionEvaluator e1 = new ExpressionEvaluator();
            w.accept(e1);
            colnames = e1.getColNames();
            for (String tab : colnames) {
                if (tab.contains(".")) {
                    tab_list.add(tab.split("\\.")[0]);
                    col_list.add(tab);
                }

            }

            if (tab_list.size() == 1) {
                String tabname = tab_list.toArray()[0].toString();
                if (tabinfo.containsKey(tabname)) {
                    List<Expression> expList = tabinfo.get(tabname);
                    expList.add(w);

                    tabinfo.put(tabname, expList);

                } else {
                    List<Expression> expList = new ArrayList<Expression>();
                    expList.add(w);
                    tabinfo.put(tabname, expList);
                }

            }
            tab_list.clear();
            col_list.clear();
        }

    }

}
