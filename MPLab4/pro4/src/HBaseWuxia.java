import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
/**
 * Created by yq-mapreduce on 5/6/17.
 */

public class HBaseWuxia {
    private static Configuration configuration = HBaseConfiguration.create();
    static{
       configuration.set("hbase.zookeeper.quorum", "localhost");
        configuration.set("hbase.zookeeper.property.clientport","2181");
    }
    //private static ArrayList<String> wordArrayList = new ArrayList<>();
    public static void main(String[] args) {
        try {
            createHBaseTable("Wuxia");
            Configuration conf = new Configuration();
            Job job = new Job(conf,"HBaseWuxia");
            job.setJarByClass(HBaseWuxia.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(InvertedIndexMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setCombinerClass(InvertedIndexCombiner.class);
            job.setReducerClass(InvertedIndexReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createHBaseTable(String tableName) throws IOException{
        TableName name= TableName.valueOf(tableName.getBytes());
        HTableDescriptor tableDescriptor=new HTableDescriptor(tableName);
        HColumnDescriptor columnDescriptor=new HColumnDescriptor("average");
        tableDescriptor.addFamily(columnDescriptor);
        //Configuration conf=HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "localhost");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin= connection.getAdmin();
        if(admin.tableExists(name)){
          //  System.out.println("表已存在，正在尝试重新创建表！");
            admin.disableTable(name);
            admin.deleteTable(name);
        }
    //    System.out.println("创建新表："+tableName);
        admin.createTable(tableDescriptor);
        connection.close();
    }


    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException
// default RecordReader: LineRecordReader; key: line offset; value: line string
        {
            StringTokenizer itr = new StringTokenizer(value.toString());
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String[] split = String.valueOf(fileSplit.getPath().getName()).split("\\.");
            String filename;
            if (split.length == 4)
                filename = split[0] + split[1];
            else
                filename = split[0];

            while (itr.hasMoreTokens()) {
                Text keyinfo = new Text(itr.nextToken() + ':' + filename);
                context.write(keyinfo, new Text("1"));
            }
        }
    }

    public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text valueinfo = new Text();
            Text keyinfo = new Text();
            int sum = 0;
            for (Text x : values) {
                sum += Integer.parseInt(x.toString());
            }
            int index = key.toString().indexOf(":");
            keyinfo = new Text(key.toString().substring(0, index));
            valueinfo = new Text(key.toString().substring(index + 1, key.toString().length()) + ':' + Integer.toString(sum));
            context.write(keyinfo, valueinfo);
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text,Text> {
        //ArrayList<Put>putlist = new ArrayList<Put>();
        Connection con;
        Table table;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
            con = ConnectionFactory.createConnection(configuration);
            table = con.getTable(TableName.valueOf("Wuxia"));
        }
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> novel = new ArrayList<String>();
            ArrayList<Integer> times = new ArrayList<Integer>();
            for (Text x : values) {
                int index = x.toString().indexOf(':');
                String novelname = x.toString().substring(0, index);
                int count = Integer.parseInt(x.toString().substring(index + 1, x.toString().length()));
                if (novel.indexOf(novelname) == -1) {
                    novel.add(novelname);
                    times.add(count);
                } else {
                    int temp = times.get(novel.indexOf(novelname));
                    temp += count;
                    times.set(novel.indexOf(novelname), temp);
                }
            }

            double sum = 0;
            for (int i : times) {
                sum += (double) i;
            }
            double avg = sum / times.size();
            String avgf = String.format("%.2f", avg);
            StringBuilder all = new StringBuilder();
            //all.append(avgf + ',');
            for (int i = 0; i < times.size(); i++) {
                all.append(novel.get(i) + ':' + Integer.toString(times.get(i)) + ';');
            }

            Put put = new Put(Bytes.toBytes(key.toString()));
            put.add(Bytes.toBytes("average"), Bytes.toBytes("average for each word"), Bytes.toBytes(avgf.toString()));
            //putlist.add(put);
            table.put(put);
            context.write(key, new Text(all.toString().substring(0, all.length() - 1)));
           // context.write(new ImmutableBytesWritable(key.toString().getBytes()), put);
        }


        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{

            table.close();
            con.close();
        }
    }

}
