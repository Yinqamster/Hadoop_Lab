import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by yq-mapreduce on 5/10/17.
 */
public class StopwordToHBase {
    static Configuration conf = HBaseConfiguration.create();
    static{
        conf.set("hbase.zookeeper.quorum", "localhost");
    }
    public static void main(String[] args){
        String tablename = "StopWords";
        try {
            String filename = "/home/yq-mapreduce/Desktop/ex4_stop_words.txt";
            createHBaseTable(tablename);
            File csv = new File(filename);
            BufferedReader br = null;
            try{
                br = new BufferedReader(new FileReader(csv));
            } catch (FileNotFoundException e){
                e.printStackTrace();
            }
            ArrayList<Put>putlist = new ArrayList<Put>();
            String everyLine = "";
            try {
                while ((everyLine = br.readLine()) != null)
                {
                    if(everyLine.isEmpty())
                        break;
                    Put put = new Put(Bytes.toBytes(everyLine.toString()));
                    put.add(Bytes.toBytes("1"), Bytes.toBytes("1"), Bytes.toBytes("1"));
                    putlist.add(put);
                }

            } catch (IOException e){
                e.printStackTrace();
            }
            HTable table = new HTable(conf, tablename);
            table.put(putlist);
            table.flushCommits();
            table.close();
        }catch(IOException e){
            e.printStackTrace();
        }
    }
    public static void createHBaseTable(String tableName) throws IOException{
        HTableDescriptor tableDescriptor=new HTableDescriptor(tableName);
        HColumnDescriptor columnDescriptor=new HColumnDescriptor("1");
        tableDescriptor.addFamily(columnDescriptor);
        Configuration conf=HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        HBaseAdmin admin=new HBaseAdmin(conf);
        if(admin.tableExists(tableName)){
            //  System.out.println("表已存在，正在尝试重新创建表！");
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        //    System.out.println("创建新表："+tableName);
        admin.createTable(tableDescriptor);
    }

}
