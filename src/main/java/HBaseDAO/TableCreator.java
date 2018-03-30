package HBaseDAO;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ILIAS on 22/1/2018.
 */

/** TableCreator, a class that creates and initialize the hBase Tables of our program
 *  This class also contains and keep the references of our tables and can be accessed
 *  from the whole program by the getters of this class */
public class TableCreator {

    private TableName RTable;
    private TableName STable;
    private TableName joinedTable;
    private Table tableR;
    private Table tableS;
    private Table joinedTableR_S;

    private Connection connectionFactory;
    private Admin hAdmin;

    /** Constructor of the class, where we save the connectionFactory and hAdmin objects for later uses
     *  Also we initialize the table names and the actual tables*/
    public TableCreator(Connection connectionFactory,Admin hAdmin) {
        this.connectionFactory = connectionFactory;
        this.hAdmin = hAdmin;
        this.RTable = TableName.valueOf("R");
        this.STable = TableName.valueOf("S");
        this.joinedTable = TableName.valueOf("JoinedTable");

        try {
            initConnFactoryTables();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /** createOrReplaceTables a method that initialize the appropriate column families of our tables
     *  and also checks the tables existence and if they exist we replace them */
    public void createOrReplaceTables(){
        try {
            List<String> listOfRTableFamilies = new ArrayList<String>();
            List<String> listOfSTableFamilies = new ArrayList<String>();
            listOfRTableFamilies.add("R1");
            listOfRTableFamilies.add("R2");
            listOfSTableFamilies.add("S1");
            listOfSTableFamilies.add("S2");

            createOrReplaceHBaseTable(this.RTable,"R",listOfRTableFamilies);
            createOrReplaceHBaseTable(this.STable,"S",listOfSTableFamilies);
            createOrReplaceJoinedTable(this.joinedTable,"JoinedTable","R_Join_S_CF");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initConnFactoryTables() throws IOException {
        this.tableR = this.connectionFactory.getTable(RTable);
        this.tableS = this.connectionFactory.getTable(STable);
        this.joinedTableR_S = this.connectionFactory.getTable(joinedTable);
    }

    /** createOrReplaceHBaseTable the method that checks if table exist and if it does
     *  it calls the appropriate method to delete
     *  and after deletion it recreates it
     *
     *  The tables that are getting created in this method are the follow :
     *  1) table name := R with column families R1,R2 (Key,Value)
     *  2) table name := S with column families S1,S2 (Key,Value) */
    private void createOrReplaceHBaseTable(TableName table, String tableName,
                                           List<String> listOfFamilies) throws IOException {

        if(this.hAdmin.tableExists(table)) { deleteTable(table); }

        HTableDescriptor hBaseTableDetails = createHBaseTableWithFamilies(listOfFamilies, tableName);
        System.out.println("Creation of HTable : "+tableName+" finished!");

        /** Creation of final HBase table with the given Column families and Table Name */
        this.hAdmin.createTable(hBaseTableDetails);

    }

    /** deleteTable is the method that replace the table */
    private void deleteTable(TableName table){
        try {
            this.hAdmin.disableTable(table);
            this.hAdmin.deleteTable(table);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /** createHBaseTableWithFamilies creates the table with column families */
    private HTableDescriptor createHBaseTableWithFamilies(List<String> listOfFamilies, String tableName){

        HTableDescriptor hBaseTableDetails = new HTableDescriptor(
                TableName.valueOf(tableName));
        hBaseTableDetails = addFamilies(listOfFamilies,hBaseTableDetails);

        return hBaseTableDetails;
    }

    /** addFamilies adds the given list of ColumnFamilies to the given table */
    private HTableDescriptor addFamilies(List<String> listOfFamilies, HTableDescriptor hBaseTableDetails){

        for(String family : listOfFamilies){
            hBaseTableDetails.addFamily(new HColumnDescriptor(family));
        }

        return hBaseTableDetails;
    }

    /** same with createOrReplaceHBaseTable but it creates a table with 1 column family := R_Join_S_CF */
    public void createOrReplaceJoinedTable(TableName table, String tableName, String columnFamily) throws IOException {

        if(this.hAdmin.tableExists(table)) { deleteTable(table); }

        HTableDescriptor hTableDesc = new HTableDescriptor(
                TableName.valueOf(tableName));

        hTableDesc.addFamily(new HColumnDescriptor(columnFamily));
        this.hAdmin.createTable(hTableDesc);

    }

    public Table getTableR() {
        return tableR;
    }

    public Table getTableS() {
        return tableS;
    }

    public Table getJoinedTableR_S() {
        return joinedTableR_S;
    }
}
