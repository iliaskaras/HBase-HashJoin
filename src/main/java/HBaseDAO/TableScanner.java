package HBaseDAO;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by ILIAS on 22/1/2018.
 */
public class TableScanner {

    /** scanTableData the method that scans the table data */
    public static ResultScanner scanTableData(Table tableToScan, String colKeyFamily, String colKey, String colValueFamily, String colValue) throws IOException {
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(colKeyFamily), Bytes.toBytes(colKey));
        scan.addColumn(Bytes.toBytes(colValueFamily), Bytes.toBytes(colValue));


        return tableToScan.getScanner(scan);

    }
}
