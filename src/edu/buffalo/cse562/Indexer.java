/**
 * 
 */

package edu.buffalo.cse562;

import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.SecondaryKeyExtractor;
import jdbm.SecondaryTreeMap;

/**
 * @author Pratik
 */
public class Indexer implements Serializable {

    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unchecked")
    public void createIndices() {

        try {
            List<Index> idx;
            for (CreateTable ct : Main.ct) {
                TableIndexInfo t = new TableIndexInfo();

                String tableName = ct.getTable().getWholeTableName()
                        .toLowerCase();
                RecordManager recman = RecordManagerFactory
                        .createRecordManager(Main.index_dir + "/" + tableName);

                List<String> primaryKeyList = new ArrayList<String>();
                List<String> secondaryKeyList = new ArrayList<String>();

                PrimaryTreeMap<String, String> primaryStringIndex = null;
                PrimaryTreeMap<Integer, String> primaryIntegerIndex = null;

                idx = ct.getIndexes();
                int count = 1;
                for (Index i : idx) {
                    if (count == 3) {
                        break;
                    }
                    count++;

                    if (i.getType().equalsIgnoreCase("PRIMARY KEY")) {
                        primaryKeyList = i.getColumnsNames();

                    } else if (i.getType().equalsIgnoreCase("INDEX")) {

                        secondaryKeyList = i.getColumnsNames();

                    }

                }
                t.primaryKeyList = primaryKeyList;
                t.secondaryKeyList = secondaryKeyList;

                // If more than one primary key:

                if (primaryKeyList.size() > 1) {
                    primaryStringIndex = recman.treeMap("primaryIndex");

                } else {
                    primaryIntegerIndex = recman.treeMap("primaryIndex");
                }

                if (secondaryKeyList.size() > 0) {

                    final int col = ConditionEvaluator.getColNumber(
                            secondaryKeyList.get(0), tableName);

                    String datatype = new DataType(secondaryKeyList.get(0))
                            .getDatatype();

                    if (primaryStringIndex != null) {
                        if (!Main.isbuild) {
                            t.primaryStringIndex = primaryStringIndex;
                        }

                        if (datatype.equalsIgnoreCase("int")) {

                            SecondaryTreeMap<Integer, String, String> secondaryIndex;

                            secondaryIndex = primaryStringIndex
                                    .secondaryTreeMap(
                                            "secondaryIndex",
                                            new SecondaryKeyExtractor<Integer, String, String>() {
                                                @Override
                                                public Integer extractSecondaryKey(
                                                        String key, String value) {
                                                    return Integer.parseInt(value
                                                            .split("\\|")[col]);
                                                }

                                            });
                            if (!Main.isbuild) {
                                t.secondaryIntegerStringIndex = secondaryIndex;
                            }

                        } else if (datatype.equalsIgnoreCase("date")) {

                            SecondaryTreeMap<Date, String, String> secondaryIndex;

                            secondaryIndex = primaryStringIndex
                                    .secondaryTreeMap(
                                            "secondaryIndex",
                                            new SecondaryKeyExtractor<Date, String, String>() {
                                                @Override
                                                public Date extractSecondaryKey(
                                                        String key, String value) {

                                                    try {

                                                        SimpleDateFormat sdf = new SimpleDateFormat(
                                                                "yyyy-MM-dd");
                                                        return sdf.parse(value
                                                                .split("\\|")[col]);
                                                    } catch (Exception e) {
                                                        e.printStackTrace();
                                                    }
                                                    return null;
                                                }
                                            });

                            if (!Main.isbuild) {
                                t.secondaryDateStringIndex = secondaryIndex;
                            }

                        } else if (datatype.equalsIgnoreCase("String")) {
                            SecondaryTreeMap<String, String, String> secondaryIndex;

                            secondaryIndex = primaryStringIndex
                                    .secondaryTreeMap(
                                            "secondaryIndex",
                                            new SecondaryKeyExtractor<String, String, String>() {
                                                @Override
                                                public String extractSecondaryKey(
                                                        String key, String value) {
                                                    return value.split("\\|")[col];
                                                }

                                            });
                            if (!Main.isbuild) {
                                t.secondaryStringStringIndex = secondaryIndex;
                            }
                        }

                        if (Main.isbuild) {
                            createPrimaryStringIndex(primaryStringIndex,
                                    primaryKeyList, tableName);
                            recman.commit();
                            recman.close();
                        } else {
                            EvaluateStatement.indices.put(tableName, t);
                        }
                    }

                    // End of more than one primary key
                    else { // if one primary key

                        if (!Main.isbuild) {
                            t.primaryIntegerIndex = primaryIntegerIndex;
                        }

                        if (datatype.equalsIgnoreCase("int")) {
                            SecondaryTreeMap<Integer, Integer, String> secondaryIndex;

                            secondaryIndex = primaryIntegerIndex
                                    .secondaryTreeMap(
                                            "secondaryIndex",
                                            new SecondaryKeyExtractor<Integer, Integer, String>() {
                                                @Override
                                                public Integer extractSecondaryKey(
                                                        Integer key,
                                                        String value) {
                                                    return Integer.parseInt(value
                                                            .split("\\|")[col]);
                                                }

                                            });
                            if (!Main.isbuild) {
                                t.secondaryIntegerIntegerIndex = secondaryIndex;
                            }
                        } else if (datatype.equalsIgnoreCase("date")) {

                            SecondaryTreeMap<Date, Integer, String> secondaryIndex;

                            secondaryIndex = primaryIntegerIndex
                                    .secondaryTreeMap(
                                            "secondaryIndex",
                                            new SecondaryKeyExtractor<Date, Integer, String>() {
                                                @Override
                                                public Date extractSecondaryKey(
                                                        Integer key,
                                                        String value) {

                                                    try {

                                                        SimpleDateFormat sdf = new SimpleDateFormat(
                                                                "yyyy-MM-dd");
                                                        return sdf.parse(value
                                                                .split("\\|")[col]);
                                                    } catch (Exception e) {
                                                        e.printStackTrace();
                                                    }
                                                    return null;
                                                }
                                            });

                            if (!Main.isbuild) {
                                t.secondaryDateIntegerIndex = secondaryIndex;
                            }

                        } else if (datatype.equalsIgnoreCase("String")) {
                            SecondaryTreeMap<String, Integer, String> secondaryIndex;

                            secondaryIndex = primaryIntegerIndex
                                    .secondaryTreeMap(
                                            "secondaryIndex",
                                            new SecondaryKeyExtractor<String, Integer, String>() {
                                                @Override
                                                public String extractSecondaryKey(
                                                        Integer key,
                                                        String value) {
                                                    return value.split("\\|")[col];
                                                }

                                            });
                            if (!Main.isbuild) {
                                t.secondaryStringIntegerIndex = secondaryIndex;
                            }
                        }
                        if (Main.isbuild) {
                            createPrimaryIntegerIndex(primaryIntegerIndex,
                                    primaryKeyList, tableName);
                            recman.commit();
                            recman.close();
                        } else {
                            EvaluateStatement.indices.put(tableName, t);
                        }
                    }

                } else {
                    // no secondary index
                    if (primaryIntegerIndex != null) {
                        t.primaryIntegerIndex = primaryIntegerIndex;
                    } else {
                        t.primaryStringIndex = primaryStringIndex;
                    }

                    if (Main.isbuild) {
                        createPrimaryIntegerIndex(primaryIntegerIndex,
                                primaryKeyList, tableName);
                        recman.commit();
                        recman.close();
                    } else {
                        EvaluateStatement.indices.put(tableName, t);
                    }

                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * @param primaryIntegerIndex
     * @param primaryKeyList
     * @param tableName
     */
    private void createPrimaryIntegerIndex(
            PrimaryTreeMap<Integer, String> primaryIntegerIndex,
            List<String> col_list, String tableName) {
        // TODO Auto-generated method stub

        int col = ConditionEvaluator.getColNumber(col_list.get(0), tableName);

        try {
            BufferedReader br = null;
            File f = new File(Main.data_dir + "/" + tableName + ".dat");
            br = new BufferedReader(new FileReader(f));
            String value;
            while ((value = br.readLine()) != null) {
                int key = Integer.parseInt(value.split("\\|")[col]);
                primaryIntegerIndex.put(key, value);

            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void createPrimaryStringIndex(
            PrimaryTreeMap<String, String> primaryIndex, List<String> col_list,
            String tableName) {

        int colNum[] = new int[col_list.size()];
        int count = 0;
        for (String col : col_list) {
            colNum[count++] = ConditionEvaluator.getColNumber(col, tableName);
        }

        try {
            BufferedReader br = null;
            File f = new File(Main.data_dir + "/" + tableName + ".dat");
            br = new BufferedReader(new FileReader(f));
            String value;
            while ((value = br.readLine()) != null) {
                String key = "";

                for (int i : colNum) {
                    key = key + value.split("\\|")[i] + "+";
                }

                key = key.substring(0, key.length() - 1);
                primaryIndex.put(key, value);
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    class DateComparator implements Serializable, Comparator<Date> {
        /**
		 * 
		 */
        private static final long serialVersionUID = 1L;

        @Override
        public int compare(Date o1, Date o2) {

            if (o1.compareTo(o2) != 0) {
                return o1.compareTo(o2);
            } else {
                return 1;
            }
        }

    }

}
