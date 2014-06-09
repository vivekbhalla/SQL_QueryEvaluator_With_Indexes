
package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;

public class EvaluateStatement {
    @SuppressWarnings("unused")
    private final FromItem from;
    private Expression where;

    public static List<Column> groupBy;
    private List<OrderByElement> orderby;
    private final Select select;
    static public List<Table> tables;
    private LinkedHashMap<Integer, List<Tuple>> outstream;
    public static Select oldselect = null;
    static int isSubselect = 0;
    // Extra
    public static HashMap<String, HashSet<String>> GH;
    public static HashMap<String, TableIndexInfo> indices;
    public static HashMap<String, String> aliasMap;
    public static boolean isLimit = false;

    //
    public EvaluateStatement(Select select) {
        //
        GH = new HashMap<String, HashSet<String>>();
        //
        this.select = select;
        from = null;
        where = null;
        groupBy = null;
        orderby = new ArrayList<OrderByElement>();
        tables = new ArrayList<Table>();
        outstream = new LinkedHashMap<Integer, List<Tuple>>();
        indices = new HashMap<String, TableIndexInfo>();
        aliasMap = new HashMap<String, String>();
    }

    /**
     * Query Processing is done here, everything from - SUBSELECT, Single or
     * Multiple Table FROM calculation, GROUP BY, SELECT & ORDER BY
     * 
     * @return Final Outstream to be printed
     * @throws ParseException
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public LinkedHashMap<Integer, List<Tuple>> processQuery()
            throws ParseException, IOException {

        PlainSelect plain_select = (PlainSelect) select.getSelectBody();
        SubSelect sub_select = null;

        FromItem from = plain_select.getFromItem();

        TablesNamesFinder t = new TablesNamesFinder();
        from.accept(t);

        // Get Table Names
        tables = t.getTableList(select);

        // Remove CT's
        List<CreateTable> tempCT = new ArrayList<CreateTable>();
        for (CreateTable ct : Main.ct) {
            for (Table tab : tables) {
                if (tab.getName().equalsIgnoreCase(ct.getTable().getName())) {
                    tempCT.add(ct);
                    break;
                }
            }
        }
        Main.ct = tempCT;

        // adding alias
        for (Table t1 : tables) {
            if (t1.getAlias() != null) {
                aliasMap.put(t1.getAlias(), t1.getName());
            }
        }

        if (plain_select.getLimit() != null) {
            isLimit = true;
        }

        sub_select = t.getSubSelect();
        // Get SUB SELECT from query if present
        List<SelectExpressionItem> selectList = new ArrayList<SelectExpressionItem>();
        selectList = plain_select.getSelectItems();
        new OutputSchema(selectList).createOutputSchema();

        if (sub_select != null) {

            Select s = new Select();
            s.setSelectBody(sub_select.getSelectBody());
            outstream = new EvaluateStatement(s).processQuery();
            oldselect = s;
            new OutputSchema(selectList).createOutputSchema();
            tables = new ArrayList<Table>();
            Main.ct.add(OutputSchema.os);

        }

        /* WHERE condition evaluation */

        /* If a Single Table is present directly apply conditions */
        if (tables.size() == 1) {
            outstream = new SingleTableIndexEvaluator(tables, plain_select)
                    .evaluate();

        }
        /*
         * If Multiple Tables are present, load them in memory if swap directory
         * is present and calculate using Nested loop join or else if swap
         * directory is present using classic Hash Join.
         */
        else if (tables.size() > 1) {
            ExpressionEvaluator e = new ExpressionEvaluator();
            where = plain_select.getWhere();
            Expression exp = where;
            exp.accept(e);
            List<Expression> where_exp = e.getExpList();

            List<IndexWrapper> ilist = new IndexCreator()
                    .createSortedIndices(where_exp);

            // for (IndexWrapper i : ilist) {
            // i.print();
            // System.out
            // .println("...........................................");
            // }

            // long endTime = System.currentTimeMillis();
            // long totalTime = endTime - Main.startTime;
            // System.out.println("Time Before Push Projection: " + totalTime);
            outstream = new MultipleTableIndexEvaluator(plain_select, ilist)
                    .evaluate();
            // for (Integer i : outstream.keySet()) {
            // for (Tuple t1 : outstream.get(i)) {
            // System.out.println(t1.getTableName());
            // t1.printTuple();
            // }
            // }
        }
        // long startTime = System.currentTimeMillis();

        /* Group By Calculation */
        groupBy = plain_select.getGroupByColumnReferences();
        if (groupBy != null) {
            if (tables.size() > 1) {
                outstream = new OperatorGroupBy(outstream, groupBy)
                        .calculateGroupBy();
            } else {
                outstream = new OperatorGroupBy(outstream, groupBy)
                        .calculateSingleGroupBy();
            }

            // long endTime = System.currentTimeMillis();
            // long totalTime = endTime - startTime;
            // System.out.println("Time After Group By: " + totalTime);

        }

        // startTime = System.currentTimeMillis();
        //
        // /* Calculate SELECT(Projection) */
        // int count = 0;
        // LinkedHashMap<Integer, List<Tuple>> new_outstream = new
        // LinkedHashMap<Integer, List<Tuple>>();
        //
        // /* If GROUP BY is present, calculate projection accordingly */
        // if (groupBy != null) {
        //
        // for (Integer i : OperatorGroupBy.sel_op.keySet()) {
        // LinkedHashMap<Integer, List<Tuple>> instream = OperatorGroupBy.sel_op
        // .get(i);
        //
        // new_outstream.put(count++, new OperatorProject(instream,
        // plain_select).calculateGroupByProjection());
        // //
        // // long endTime = System.currentTimeMillis();
        // // long totalTime = endTime - Main.startTime;
        // // System.out.println("Time After Each Group: " + totalTime /
        // // 1000
        // // + " seconds");
        //
        // }
        // outstream = new_outstream;
        //
        // }

        /* Calculate SELECT(Projection) */
        // startTime = System.currentTimeMillis();

        LinkedHashMap<Integer, List<Tuple>> new_outstream = new LinkedHashMap<Integer, List<Tuple>>();

        /* If GROUP BY is present, calculate projection accordingly */
        if (groupBy != null) {

            Thread[] threads = new Thread[OperatorGroupBy.sel_op.keySet()
                    .size()];
            OperatorProject[] ops = new OperatorProject[OperatorGroupBy.sel_op
                    .keySet().size()];
            for (Integer i : OperatorGroupBy.sel_op.keySet()) {
                LinkedHashMap<Integer, List<Tuple>> instream = OperatorGroupBy.sel_op
                        .get(i);
                OperatorProject op = new OperatorProject(instream, plain_select);
                ops[i] = op;
                Thread task = new Thread(op);
                threads[i] = task;
                task.start();

                // new_outstream.put(count++, new OperatorProject(instream,
                // plain_select).calculateGroupByProjection());

            }

            int groupCount = 0;
            for (Thread task : threads) {
                try {
                    task.join();
                    new_outstream.put(groupCount, ops[groupCount].tlist_thread);
                    groupCount++;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            outstream = new_outstream;

        }

        /* If GROUP BY is not present, calculate projection normally */
        else {

            outstream = new OperatorProject(outstream, plain_select)
                    .calculateProjection();

        }

        // long endTime = System.currentTimeMillis();
        // long totalTime = endTime - startTime;
        // System.out.println("Time After Projection: " + totalTime);

        // startTime = System.currentTimeMillis();
        /* Calculate ORDER BY */
        orderby = plain_select.getOrderByElements();
        if (orderby != null) {
            /*
             * To start from last ORDER BY element (Because that is the SQL
             * Syntax), reverse the ORDER BY list.
             */
            Collections.reverse(orderby);

            for (OrderByElement o : orderby) {
                outstream = new OperatorOrderBy(outstream, o)
                        .calculateOrderBy();
            }
        }

        // endTime = System.currentTimeMillis();
        // totalTime = endTime - startTime;
        // System.out.println("Time After Orderby: " + totalTime);
        return outstream;
    }

    public static String getLargestTable() {
        String ltab = null;
        long maxsize = 0;
        for (Table t : tables) {
            File f1 = new File(Main.data_dir + "/" + t.getWholeTableName()
                    + ".dat");
            if (!f1.exists()) {
                f1 = new File(Main.swap_dir + "/" + t.getWholeTableName()
                        + ".dat");
            }
            if (f1.exists()) {
                if (maxsize < f1.length()) {
                    ltab = t.getWholeTableName();
                    maxsize = f1.length();
                }
            }
        }
        return ltab;
    }

    public static String getSmallestTable() {
        String ltab = null;
        long minsize = Long.MAX_VALUE;
        for (Table t : tables) {
            File f1 = new File(Main.data_dir + "/" + t.getWholeTableName()
                    + ".dat");
            if (!f1.exists()) {
                f1 = new File(Main.swap_dir + "/" + t.getWholeTableName()
                        + ".dat");
            }

            if (f1.exists()) {
                if (minsize > f1.length()) {
                    ltab = t.getWholeTableName();
                    minsize = f1.length();
                }
            }
        }
        return ltab;
    }

    public static long getTableSize(String tab) {

        File f1 = new File(Main.data_dir + "/" + tab + ".dat");
        return f1.length() / (1024 * 1024);
    }

    private void addSchemaToMainct() {

        HashMap<String, List<String>> tablecount = new HashMap<String, List<String>>();
        for (Table t : tables) {

            if (tablecount.containsKey(t.getWholeTableName())) {

                tablecount.get(t.getWholeTableName()).add(t.getAlias());

            } else {
                List<String> l = new ArrayList<String>();
                l.add(t.getAlias());
                tablecount.put(t.getWholeTableName(), l);
            }
        }

        List<CreateTable> clist = new ArrayList<CreateTable>();
        List<Integer> remlist = new ArrayList<Integer>();
        List<Integer> tabrem = new ArrayList<Integer>();

        int count = 0;
        for (CreateTable ct : Main.ct) {
            List<String> tabcnt = tablecount.get(ct.getTable()
                    .getWholeTableName().toLowerCase());

            if (tabcnt.size() > 1) {
                for (String nt : tabcnt) {
                    Table t = new Table();
                    CreateTable c = new CreateTable();
                    c.setColumnDefinitions(ct.getColumnDefinitions());
                    t.setName(nt);

                    c.setTable(t);
                    clist.add(c);

                    tables.add(t);
                }
                remlist.add(count);

                int cnt = 0;
                for (Table t1 : tables) {

                    if (t1.getName().equalsIgnoreCase(ct.getTable().getName())) {
                        tabrem.add(cnt);
                    }
                    cnt++;
                }

            }
            count++;
        }

        for (int i : remlist) {
            String copy = Main.ct.get(i).getTable().getWholeTableName();
            File source = new File(Main.data_dir + "/" + copy + ".dat");
            for (String dest : tablecount.get(copy.toLowerCase())) {
                File d = new File(Main.swap_dir + "/" + dest + ".dat");
                try {
                    Files.copy(source.toPath(), d.toPath());
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }

        int c = 0;
        for (int i : tabrem) {
            tables.remove(i - c);
            c++;
        }

        int c1 = 0;
        for (int i : remlist) {
            Main.ct.remove(i - c1);
            c1++;
        }
        for (CreateTable ct : clist) {
            Main.ct.add(ct);
        }

    }
}
