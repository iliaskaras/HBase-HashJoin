import HBaseDAO.TableCreator;
import HBaseDAO.TableRowRecorder;
import JoinHandler.HashJoinAlgorithm;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

/**
 * Created by ILIAS on 21/1/2018.
 */

/** Details for each calling function in details can be found inside their classes
 *  tableFiller.fillTheTables(10,500) at line 27, we specify length of small table and large table respectively */
public class HashJoinHandler {
    public static void main(String[] args) {
        org.apache.log4j.BasicConfigurator.configure();
        Configuration conf = HBaseConfiguration.create();

        try {
            Connection connectionFactory = ConnectionFactory.createConnection(conf);
            Admin hAdmin = connectionFactory.getAdmin();

            TableCreator tableCreator = new TableCreator(connectionFactory,hAdmin);
            tableCreator.createOrReplaceTables();

            TableRowRecorder tableFiller = new TableRowRecorder(tableCreator);
            tableFiller.fillTheTables(10,500);

            HashJoinAlgorithm hashJoin = new HashJoinAlgorithm(tableCreator.getTableR(),
                                                               tableCreator.getTableS(),
                                                               tableCreator.getJoinedTableR_S());
            hashJoin.executeHashJoin();


        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
