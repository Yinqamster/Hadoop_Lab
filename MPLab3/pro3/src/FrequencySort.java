import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by zarah on 4/22/17.
 */
public class FrequencySort {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = new Job(conf,"FrequencySort");
            job.setJarByClass(FrequencySort.class);
            job.setMapperClass(FrequencySortMapper.class);
            job.setMapOutputKeyClass(DoubleWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(FrequencySortReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        catch (Exception e) { e.printStackTrace();}
    }
}

class FrequencySortMapper extends Mapper<Object,Text,DoubleWritable,Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
// default RecordReader: LineRecordReader; key: line offset; value: line string
    {
        DoubleWritable keyInfo = new DoubleWritable();
        Text valueInfo = new Text();
        String s = value.toString();
        String buf1= s.substring(s.indexOf('\t')+1,s.indexOf(','));
        keyInfo.set(Double.parseDouble(buf1));
        String buf2= s.substring(0,s.indexOf('\t'))+s.substring(s.indexOf(','),s.length());
        valueInfo.set(buf2);
        context.write(keyInfo,valueInfo);

    }
}

class FrequencySortReducer extends Reducer<DoubleWritable,Text,Text,Text> {
    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        Text keyInfo = new Text();
        Text valueInfo = new Text();
        for(Text x :values){
            String s = x.toString();
            String buf1=s.substring(0,s.indexOf(','));
            String buf2=key.toString()+s.substring(s.indexOf(','),s.length());
            keyInfo.set(buf1);
            valueInfo.set(buf2);
            context.write(keyInfo,valueInfo);
        }
    }
}