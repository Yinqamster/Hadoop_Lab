import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.swing.text.html.HTMLDocument;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yq-mapreduce on 7/16/17.
 */

public class Task1to4 {
    public static class MyMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) {
            try {
                String Onelog = value.toString().trim();
                String[] partwords = Split.splitOnelog(Onelog);

                Text keyInfo;

                //task1
                if (partwords[4] != null && partwords[1] != null) {
                    int index = partwords[1].indexOf(':');
                    String hour = partwords[1].substring(index + 1, index + 3);
                    String task1str = "1:" + partwords[4] + ":" + hour;
                    keyInfo = new Text(task1str);
                    context.write(keyInfo, new Text("1"));
                }

                //task2
                if (partwords[0] != null && partwords[1] != null) {
                    int index = partwords[1].indexOf(':');
                    String hour = partwords[1].substring(index + 1, index + 3);
                    String task2str = "2:" + partwords[0];
                    keyInfo = new Text(task2str);
                    context.write(keyInfo, new Text(hour));
                }

                //task3
                if (partwords[2] != null && partwords[1] != null) {
                    int index = partwords[1].indexOf(' ');
                    String time = partwords[1].substring(1, index);

                    String[] buf = partwords[2].split(" ");
                    String task3str = "3:" + buf[1];
                    keyInfo = new Text(task3str);
                    context.write(keyInfo, new Text(time));
                }

                //task4
                if (partwords[1] != null && partwords[2] != null && partwords[6] != null) {
                    int index = partwords[1].indexOf(':');
                    String hour = partwords[1].substring(index + 1, index + 3);

                    String[] buf = partwords[2].split(" ");
                    String task4str = "4:" + buf[1];
                    keyInfo = new Text(task4str);
                    context.write(keyInfo, new Text(hour + ":" + partwords[6]));
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, NullWritable> {

        //task1
        Map<String,Integer>Status = new TreeMap<String,Integer>();
        Map<String, Integer>[] StatusCode = new TreeMap[24];

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //task1
            for (int i = 0; i < 24; i++) {
                StatusCode[i] = new TreeMap<String, Integer>();
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) {
            try {
                String part[] = key.toString().split("\\:");

                //task1
                if (part[0].equals("1")) {
                    int sum = 0;
                    for (Text x : values) {
                        sum += Integer.parseInt(x.toString());
                    }

                    if(Status.containsKey(part[1])) {
                        int times = Status.get(part[1]);
                        times+=sum;
                        Status.put(part[1],times);
                    }
                    else
                        Status.put(part[1],sum);

                    int hour = Integer.parseInt(part[2]);
                    StatusCode[hour].put(part[1], sum);
                }

                //task2
                if (part[0].equals("2")) {
                    int[] hourtimes = new int[24];
                    for (int i = 0; i < 24; i++) {
                        hourtimes[i] = 0;
                    }
                    int sum = 0;
                    for (Text x : values) {
                        sum++;
                        hourtimes[Integer.parseInt(x.toString())]++;
                    }
                    //key:   2:IP|keyvalue
                    String prefix = "2:" + part[1] + '|';
                    context.write(new Text(prefix + part[1] + ":" + String.valueOf(sum)), null);
                    for (int i = 0; i < 24; i++) {
                        if (hourtimes[i] != 0) {
                            String hour = String.valueOf(i) + ":00-" + String.valueOf((i + 1)%24) + ":00";
                            String buf = prefix + hour + " " + String.valueOf(hourtimes[i]);
                            context.write(new Text(buf), null);
                        }
                    }
                }

                //task3
                if (part[0].equals("3")) {
                    Map<String, Integer> secondtimes = new TreeMap<String, Integer>();
                    int sum = 0;
                    for (Text x : values) {
                        sum++;
                        if (secondtimes.containsKey(x.toString())) {
                            secondtimes.put(x.toString(), secondtimes.get(x.toString()) + 1);
                        } else
                            secondtimes.put(x.toString(), 1);
                    }
                    //key:   3:url|keyvalue
                    String prefix = "3:" + part[1] + '|';
                    context.write(new Text(prefix + part[1] + ":" + String.valueOf(sum)), null);

                    Iterator<String>Iter = secondtimes.keySet().iterator();
                    while(Iter.hasNext()){
                        String s = Iter.next();
                        context.write(new Text(prefix + s + ' ' + String.valueOf(secondtimes.get(s))), null);
                    }
                }

                //task4
                if (part[0].equals("4")) {
                    int[] hourtimes = new int[24];
                    int[] hourlong = new int[24];
                    double[] houravg = new double[24];
                    for (int i = 0; i < 24; i++) {
                        hourtimes[i] = 0;
                        hourlong[i] = 0;
                    }
                    int sumtimes = 0;
                    int sumlong = 0;
                    double sumavg = 0;
                    for (Text x : values) {
                        sumtimes++;
                        String[] val = x.toString().split(":");
                        int hour = Integer.parseInt(val[0]);
                        int Long = Integer.parseInt(val[1]);
                        sumlong += Long;
                        hourtimes[hour]++;
                        hourlong[hour] += Long;
                    }
                    sumavg = (double) sumlong / (double) sumtimes;
                    for (int i = 0; i < 24; i++) {
                        houravg[i] = (double) hourlong[i] / (double) hourtimes[i];
                    }
                    //key:   4:url|keyvalue
                    String prefix = "4:" + part[1] + '|';
                    context.write(new Text(prefix + part[1] + ":" + String.format("%.2f", sumavg)), null);
                    for (int i = 0; i < 24; i++) {
                        if (hourtimes[i] != 0) {
                            String hour = String.valueOf(i) + ":00-" + String.valueOf((i + 1)%24) + ":00";
                            String buf = prefix + hour + " " + String.format("%.2f", houravg[i]);
                            context.write(new Text(buf), null);
                        }
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //task1
            //1:keyvalue
            for(String s:Status.keySet()) {
                context.write(new Text("1:" + s + ":" + String.valueOf(Status.get(s))), null);
            }
            for (int i = 0; i < 24; i++) {
                String hour = String.valueOf(i) + ":00-" + String.valueOf((i + 1) % 24) + ":00";
                String info="";
                for(String s:Status.keySet()) {
                    String buf;
                    if (StatusCode[i].containsKey(s))
                        buf = s + ":" + String.valueOf(StatusCode[i].get(s));
                    else
                        buf = s + ":0";
                    info += buf + ' ';
                }

                context.write(new Text("1:" + hour + ' ' + info), null);
            }

        }
    }


    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            conf.set("1", args[1]);
            conf.set("2", args[2]);
            conf.set("3", args[3]);
            conf.set("4", args[4]);

            Job job = new Job(conf);
            job.setJarByClass(Task1to4.class);

            job.setMapperClass(MyMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            //    job.setCombinerClass(MyCombiner.class);
            job.setReducerClass(MyReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(VVLogNameMultipleTextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
