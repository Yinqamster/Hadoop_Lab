import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by zarah on 4/22/17.
 */
public class TFIDF {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = new Job(conf,"TFIDF");
            job.setJarByClass(TFIDF.class);
            job.setMapperClass(TFIDFMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(TFIDFReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        catch (Exception e) { e.printStackTrace();}
    }
   // static ArrayList<String>Allnovels=new ArrayList<String>();
}

class TFIDFMapper extends Mapper<Object,Text,Text,Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
// default RecordReader: LineRecordReader; key: line offset; value: line string
    {
        ArrayList<String>Authors = new ArrayList<String>();
        ArrayList<Integer>times = new ArrayList<Integer>();
        int count=0;
        String s = value.toString();
        String buf= s.substring(s.indexOf(',')+1,s.length());

        int end = buf.indexOf(';',0);
        int begin=0;
        while(end!=-1||begin<buf.length()){
            if(end==-1)
                end=buf.length();
            String novel = buf.substring(begin,end);
            int i=0;
            for(;i<novel.length();i++){
                if(Character.isDigit(novel.charAt(i))){
                    break;
                }
            }
            String author = novel.substring(0,i);
            int index = Authors.indexOf(author);
            int num = Integer.parseInt(novel.substring(novel.indexOf(':')+1,novel.length()));
            if(index==-1){
                Authors.add(author);
                times.add(num);
            }
            else{
                int val=times.get(index);
                val+=num;
                times.set(index,val);
            }

            //count num of novels to cal IDE
         /*   String allnovel = novel.substring(0,novel.indexOf(':'));
            if(TFIDF.Allnovels.indexOf(allnovel)==-1){
                TFIDF.Allnovels.add(allnovel);
            }*/

            count++;
            begin=end+1;
            end=buf.indexOf(';',begin);
        }

        //output
        for(int k=0;k<times.size();k++){
            String keyInfo= Authors.get(k).toString()+'\t'+s.substring(0,s.indexOf('\t'));
            String valueInfo = Integer.toString(times.get(k)) + ':' + Integer.toString(count);
            context.write(new Text(keyInfo),new Text(valueInfo));
        }
    }
}

class TFIDFReducer extends Reducer<Text,Text,Text,Text> {


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        int allnovelnum=218;
      //  allnovelnum = TFIDF.Allnovels.size();
        //if(allnovelnum==0)
        //int allnovelnum=Integer.parseInt(context.getProfileParams())-1;
        for (Text x :values){
            String s =x.toString();
            String IF = s.substring(0,s.indexOf(':'));
            int count = Integer.parseInt(s.substring(s.indexOf(':')+1,s.length()));
            double IDE = Math.log((double)allnovelnum/(double)(count+1)) /Math.log(2);
            String valueInfo = IF + '-' + Double.toString(IDE);
            context.write(key,new Text(valueInfo));
        }
    }
}