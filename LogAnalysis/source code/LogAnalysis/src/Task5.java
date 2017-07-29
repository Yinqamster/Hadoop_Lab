import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;


/**
 * Created by yq on 7/18/17.
 */
public class Task5 {
    public static class MyMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Mapper.Context context) {
            try {
                String Onelog = value.toString().trim();
                String partwords[] = Split.splitOnelog(Onelog);
                InputSplit inputSplit = context.getInputSplit();
                String filename = ((FileSplit)inputSplit).getPath().getName();
                if (partwords[2] != null && partwords[1] != null) {
                    int index = partwords[1].indexOf(':');
                    String hour = partwords[1].substring(index + 1, index + 3);
                    String buf[] = partwords[2].split(" ");
                    String url = buf[1];
                    String task5str = hour + ":" + url + ":" + filename;
                    Text keyInfo = new Text(task5str);
                    context.write(keyInfo, new Text("1"));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class MyCombiner extends Reducer<Text,Text,Text,Text>{
        protected void reduce(Text key, Iterable<Text>values ,Context context){
            try {
                int sum = 0;
                for (Text x : values) {
                    sum += Integer.parseInt(x.toString());
                }
                Text valueInfo = new Text(Integer.toString(sum));
                context.write(key, valueInfo);
            }catch(Exception e){
                e.printStackTrace();
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, NullWritable> {
        private static int fileNum = 14;
        private static int hourNum = 24;
        private  static HashMap<String, Integer>[][] dateList = new HashMap[fileNum][hourNum];
        //predict value of date 09-22
        private static HashMap<String, Integer>[] predict22 = new HashMap[24];
        //true value of date 09-22
        private static HashMap<String, Integer>[] result = new HashMap[24];

        protected void setup(Context context) throws IOException, InterruptedException {
            for (int i = 0; i < fileNum; i++) {
                for (int j = 0; j < hourNum; j++) {
                    dateList[i][j] = new HashMap<String, Integer>();
                }
            }
            for (int i = 0; i < hourNum; i++) {
                result[i] = new HashMap<String, Integer>();
            }
        }

        protected void reduce(Text key, Iterable<Text> values, Context context) {
            try {
                String part[] = key.toString().split("\\:");
                int hour = Integer.parseInt(part[0]);
                String url = part[1];
                String filename = part[2];
            //    context.write(new Text("filename:"+filename),null);
                int date = Integer.parseInt(filename.substring(8,10));
                int sum = 0;
                Iterator it = values.iterator();
                while (it.hasNext()) {
                    Text x = (Text) it.next();
                    sum += Integer.parseInt(x.toString());
                }
                if (date < 22) {
                    dateList[date - 8][hour].put(url, Integer.valueOf(sum));
                }
                else if(date == 22) {
                    result[hour].put(url, Integer.valueOf(sum));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public double computeRMSE() {
            double RMSE = 0;
            for(int i = 0; i < hourNum; i++) {
                int numOfPre = predict22[i].size();
                int numOfRes = result[i].size();
                double sum = 0;
                Iterator it = result[i].keySet().iterator();
                while(it.hasNext()) {
                    String url = (String) it.next();
                    int trueValue = result[i].get(url);
                    int preValue = 0;
                    if(predict22[i].containsKey(url)) {
                        preValue = predict22[i].get(url);
                    }
                    sum += Math.pow(preValue-trueValue,2);
                }
                sum = sum / numOfRes;
                RMSE += Math.sqrt(sum);
            }
            RMSE = RMSE / hourNum;
            return RMSE;
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            //zhong jian wen jian shu chu
         /*   for (int i = 0; i < fileNum; i++) {
                String pre = i < 2 ? "2015-09-0" : "2015-09-";
                String date = pre+String.valueOf(i+8);
                context.write(new Text(date), null);
                for (int j = 0; j < hourNum; j++) {
                    String hour = String.valueOf(j) + ":00-" + String.valueOf(j + 1) + ":00";
                    String task5str = hour;
                    Iterator it = dateList[i][j].keySet().iterator();
                    while (it.hasNext()) {
                        String url = (String) it.next();
                        task5str += " " + url + ":" + String.valueOf(dateList[i][j].get(url));
                    }
                    context.write(new Text(task5str), null);
                }
            }*/

          //  predict22=Predict.predict(hourNum,fileNum,dateList);
            predict22=Predict.predictNew(hourNum,fileNum,dateList);
            double RMSE = computeRMSE();
            context.write(new Text("RMSE:"+String.valueOf(RMSE)),null);
            for (int i = 0; i < hourNum; i++) {
                String hour = String.valueOf(i) + ":00-" + String.valueOf((i+1)%24) + ":00";
                //context.write(new Text(hour),null);
                String task5str = hour;
                Iterator it = predict22[i].keySet().iterator();
                while(it.hasNext()) {
                    String url = (String) it.next();
                    task5str += " " + url + ":" + String.valueOf(predict22[i].get(url));
         /*           String task5str = url + "\t" + String.valueOf(predict22[i].get(url)) + "\t" + String.valueOf(urlTimes[i].get(url))
                            + "\tave:" + String.valueOf(averageTimes[i].get(url)) + "\tvar:"
                            + String.valueOf(varienceTimes[i].get(url)) + "\tmax:" + String.valueOf(maxTime[i].get(url)) + "\tmin:" + String.valueOf(minTime[i].get(url))
                            +"\trate:" + String.valueOf(changeRate[i].get(url)) + "\toutflag:" + String.valueOf(outflag[i].get(url));
                    context.write(new Text(task5str),null)
                    ;*/
                }
                context.write(new Text(task5str),null);
            }
        }
    }

    public static void main(String args[]) {
        try {
            Configuration jobconf1 = new Configuration();

                    Job job = new Job(jobconf1);
                    job.setJarByClass(Task5.class);

                    job.setMapperClass(Task5.MyMapper.class);
                    job.setMapOutputKeyClass(Text.class);
                    job.setMapOutputValueClass(Text.class);
                    job.setCombinerClass(MyCombiner.class);
                    job.setReducerClass(Task5.MyReducer.class);
                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(NullWritable.class);

                    FileInputFormat.addInputPath(job, new Path(args[0]));
                    FileOutputFormat.setOutputPath(job, new Path(args[1]));

                    System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
