package HBaseDAO;

import RandomGenerator.RandomNumberGenerator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Created by ILIAS on 22/1/2018.
 */

/** TableRowRecorder, a class that creates and fills the hBase Tables of our program */
public class TableRowRecorder {
    private TableCreator tableCreator;

    public TableRowRecorder(TableCreator tableCreator) {
        this.tableCreator = tableCreator;
    }

    /** fillTheTables, arguments : int sizeOfSmallRelation, int sizeOfLargeRelation
     *  sizeOfSmallRelation := the size of the small relation table, in our case we set it to be the R
     *  sizeOfLargeRelation := the size of the large relation table, in our case we set it to be the S
     *  This method fills the tables with key and values where =>
     *  => key := i modulo 3
     *  => value R := in R table values are one character of alphabet in lowercase
     *  => value S := in R table values are one character of alphabet in uppercase */
    public void fillTheTables(int sizeOfSmallRelation, int sizeOfLargeRelation){

        ArrayList<Integer> listOfSmallTableKeys = new ArrayList();

        /** Filling the Small Table of Relation R
         *  Contains as values LowerCase of random generated characters in alphabet
         *  The keys are generated randomly in the range of 0 to sizeOfSmallRelation */
        for(int i=1; i<=sizeOfSmallRelation; i++)
        {
            int key = RandomNumberGenerator.randBetween(0,sizeOfSmallRelation);

            listOfSmallTableKeys.add(key);

            Random rnd = new Random();
            char value = (char) (rnd.nextInt(26) + 'a');

            try {
                fillTable(this.tableCreator.getTableR(),"R1","R2","Key","Value","R_row_"+i,Integer.toString(key),Character.toString(value));

            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        Set uniqueKeyValuesOfR = new HashSet(listOfSmallTableKeys);
        RandomNumberGenerator.setUniqueKeyValuesOfR(uniqueKeyValuesOfR);


        /** Filling the large Table of Relation S
         *  Contains as values UpperCase of random generated chars
         *  The keys are generated based on modulo with 3, same as the small relation R*/
        for(int i=1; i<=sizeOfLargeRelation; i++)
        {

            int key;
            key = RandomNumberGenerator.getRandomKey(0,sizeOfLargeRelation);

            Random rnd = new Random();
            char value = (char) (rnd.nextInt(26) + 'a');
            value -= 32;

            try {
                fillTable(this.tableCreator.getTableS(),"S1","S2","Key","Value","S_row_"+i,Integer.toString(key),Character.toString(value));
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        System.out.println("Tables are Loaded!");

    }

    /** fillTables, the method that gets called from fillTheTables method
     *  and here we are putting the actual created row into the table */
    private void fillTable(Table tableToFill, String columnFamily1, String columnFamily2,
                           String columnQualifier1, String columnQualifier2,
                           String rowValue, String key, String value) throws IOException {

        Put put = new Put(Bytes.toBytes(rowValue));

        put.addColumn(Bytes.toBytes(columnFamily1), Bytes.toBytes(columnQualifier1), Bytes.toBytes(key));
        put.addColumn(Bytes.toBytes(columnFamily2), Bytes.toBytes(columnQualifier2), Bytes.toBytes(value));

        tableToFill.put(put);
    }

    /** fillFinalResultTable, the method that gets called from HashJoinAlgorithm class and method hashJoin,
     *  and here we are putting the final created joined rows into the 'JoinedTable' table
     *  3 Columns are getting saved into the ColumnFamily 'R_Join_S_CF'
     *  1 := the Key that joined the tuples
     *  2 := the R_rowID that took part in the join operation
     *  3 := the S_rowID that took part in the join operation */
    public static void fillFinalResultTable(Table tableToFill, String columnFamily, String columnKeyQualifier,
                                            String columnRowR_ID, String columnRowS_ID,
                                            String rowValue, String key, String R_RowID, String S_RowID) throws IOException {

        Put put = new Put(Bytes.toBytes(rowValue));

        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnKeyQualifier), Bytes.toBytes(key));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnRowR_ID), Bytes.toBytes(R_RowID));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnRowS_ID), Bytes.toBytes(S_RowID));

        tableToFill.put(put);
    }

}
