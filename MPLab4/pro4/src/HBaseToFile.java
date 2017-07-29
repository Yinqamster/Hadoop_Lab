import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;


/**
 * Created by zarah on 5/6/17.
 */

public class HBaseToFile {
    static Configuration conf = HBaseConfiguration.create();
    static{
        conf.set("hbase.zookeeper.quorum", "localhost");
    }
    public static void main(String[] args){
        try {
            String filename = "/home/yq-mapreduce/Desktop/HBaseOut.txt";
            String tablename = "Wuxia";
            //String filename = "/home/yq-mapreduce/Desktop/WithStopWordsOut.txt";
            //String tablename = "WuxiaStopWords";
            PrintWriter output = new PrintWriter(new File(filename));
            Scan scan = new Scan();
            ResultScanner rs=null;
            HTable table = new HTable(conf, tablename);
            try {
                rs = table.getScanner(scan);
                for (Result r : rs) {
                    for (KeyValue kv : r.list()) {
                        output.printf("%s\t%s\r\n", Bytes.toString(kv.getRow()), Bytes.toString(kv.getValue()));
                        output.flush();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                rs.close();
            }
            output.close();
        }catch(IOException e){
            e.printStackTrace();
        }
    }
}


