package JoinHandler;

import HBaseDAO.TableRowRecorder;
import HBaseDAO.TableScanner;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * Created by ILIAS on 22/1/2018.
 */

/** HashJoinAlgorithm the class that we perform the actual single-pass hash join */
public class HashJoinAlgorithm {


    private Map<String, List<String>> hashTable = new HashMap<String, List<String>>();
    private List<String> groupedValueListOfR = new ArrayList<String>();
    private List<String[]> keyValuePairListS = new ArrayList();

    private Table tableR,tableS,joinedTableR_S;


    public HashJoinAlgorithm(Table tableR, Table tableS, Table joinedTableR_S) {
        this.tableR = tableR;
        this.tableS = tableS;
        this.joinedTableR_S = joinedTableR_S;
    }

    public void executeHashJoin() throws IOException {

        buildPhase();
        probePhase();
        hashJoin();
    }

    /** The first phase and the creation of our build input Hash table
     *  The smaller relation is taken here for the creation of the build input
     *  We group the values by keys for HashMap to work (unique keys) */
    private void buildPhase() throws IOException {

        System.out.println("Build Phase Started!");
        ResultScanner tableScanner = TableScanner.scanTableData(this.tableR,"R1","Key","R2","Value");
        for (Result result = tableScanner.next(); result != null; result = tableScanner.next()){
            String currentKey = null;
            String currentRowId = null;

            for(Cell cell : result.rawCells()) {

                byte[] keyValue = CellUtil.cloneValue(cell);
                byte[] columnFamily = CellUtil.cloneFamily(cell);
                byte[] rowID = CellUtil.cloneRow(cell);

                if(Bytes.toString(columnFamily).equals("R1"))
                    currentKey = Bytes.toString(keyValue);
                else
                    currentRowId = Bytes.toString(rowID);

            }

            if(this.hashTable.get(currentKey)==null){
                this.groupedValueListOfR.add(currentRowId);
                this.hashTable.put(currentKey, this.groupedValueListOfR);
                this.groupedValueListOfR = new ArrayList<String>();
            }
            else if (this.hashTable.get(currentKey).isEmpty()){
                this.groupedValueListOfR.add(currentRowId);
                this.hashTable.put(currentKey, this.groupedValueListOfR);
                this.groupedValueListOfR = new ArrayList<String>();
            } else {
                this.groupedValueListOfR = this.hashTable.get(currentKey);
                this.groupedValueListOfR.add(currentRowId);
                this.hashTable.put(currentKey, this.groupedValueListOfR);
                this.groupedValueListOfR = new ArrayList<String>();
            }

        }

        tableScanner.close();
        System.out.println("Final hashTable of relation R, build phase finished! ");
        System.out.println(Collections.singletonList(this.hashTable));

    }

    /** The second phase and the creation of our probe input table
     *  The larger relation is taken here for the creation of the probe input
     *  Initialize keyValuePairListS with the tuples of larger relation S */
    private void probePhase() throws IOException {

        System.out.println("Probe Phase Started!");

        ResultScanner tableScanner = TableScanner.scanTableData(this.tableS,"S1","Key","S2","Value");
        for (Result result = tableScanner.next(); result != null; result = tableScanner.next()){
            String currentKey = null;
            String currentRowId = null;

            for(Cell cell : result.rawCells()) {

                byte[] keyValue = CellUtil.cloneValue(cell);
                byte[] columnFamily = CellUtil.cloneFamily(cell);
                byte[] rowID = CellUtil.cloneRow(cell);

                if(Bytes.toString(columnFamily).equals("S1"))
                    currentKey = Bytes.toString(keyValue);
                else
                    currentRowId = Bytes.toString(rowID);
            }

            this.keyValuePairListS.add(new String[]{currentKey, currentRowId});
        }
        tableScanner.close();
    }

    /** hashJoin is the method that joins our tables
     *  this.keyValuePairListS contains the tuples of our large table
     *  this.hashTable contains the small table's tuples created and initialized at build phase
     *
     *  The method loops through the tuples of keyValuePairListS and checks if there is a tuple
     *   in our hashTable with same key, if there is then
     *   :=> get all values from the hashTable associated with that key
     *   :=> perform the join by filling the 'JoinedTable' */
    private void hashJoin() throws IOException {
        List<String> listOfFoundHashKeyValues = new ArrayList();
        int rowNumber = 1;
        for (String[] keyValuePairList : this.keyValuePairListS) {

            if(this.hashTable.get(keyValuePairList[0])!=null){
                listOfFoundHashKeyValues = this.hashTable.get(keyValuePairList[0]);
                for(String currentHashValue : listOfFoundHashKeyValues){

                    TableRowRecorder.fillFinalResultTable(this.joinedTableR_S,"R_Join_S_CF","Key","R_rowID","S_rowID",
                                                            "R_S_row"+rowNumber,keyValuePairList[0],
                                                            currentHashValue,keyValuePairList[1]);
                    rowNumber +=1;

                }
            }

        }


    }

}
