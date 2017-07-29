import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by yq-mapreduce on 7/17/17.
 */
public class VVLogNameMultipleTextOutputFormat extends MultipleOutputFormat<Text, NullWritable> {
    @Override
    protected String generateFileNameForKeyValue(Text key, NullWritable value, Configuration conf) {
        String Path1=conf.get("1");
        String Path2=conf.get("2");
        String Path3=conf.get("3");
        String Path4=conf.get("4");

        String filename;
        if(key.toString().charAt(0)=='1')
            filename = Path1+"/1.txt";
        else if(key.toString().charAt(0)=='2') {
            int index = key.toString().indexOf('|');
            filename = Path2+"/"+key.toString().substring(2,index)+".txt";
        }
        else if(key.toString().charAt(0)=='3') {
            int index = key.toString().indexOf('|');
            String bufname = key.toString().substring(3,index);
            bufname=bufname.replace('/','-');
            filename = Path3+"/"+bufname+".txt";
        }
        else if(key.toString().charAt(0)=='4') {
            int index = key.toString().indexOf('|');
            String bufname = key.toString().substring(3,index);
            bufname=bufname.replace('/','-');
            filename = Path4+"/"+bufname+".txt";
        }
        else
            filename = "error";
        return filename;
    }
}
