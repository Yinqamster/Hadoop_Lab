import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yq on 7/22/17.
 */
public class Split {
    public static String[] splitOnelog(String log) {
        String[] partwords = new String[7];

        String[] splitwords = log.split(" ");
        partwords[6] = splitwords[splitwords.length - 1];
        partwords[5] = splitwords[splitwords.length - 2];
        partwords[4] = splitwords[splitwords.length - 3];
        partwords[3] = splitwords[splitwords.length - 4];

        Pattern pattern0 = Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");
        Pattern pattern1 = Pattern.compile("(\\[).*?(\\])");
        Pattern pattern2 = Pattern.compile("(\").*?(\")");

        Matcher matcher = pattern0.matcher(log);
        if (matcher.find()) partwords[0] = matcher.group();
        matcher = pattern1.matcher(log);
        if (matcher.find()) partwords[1] = matcher.group();
        matcher = pattern2.matcher(log);
        if (matcher.find()) partwords[2] = matcher.group();
        else {
            partwords[2] = splitwords[splitwords.length - 6] + " " + splitwords[splitwords.length - 5];
            if (splitwords[splitwords.length - 5].equals("null"))
                partwords[2] = null;
        }

        for (int i = 3; i < 7; i++)
            if (partwords[i].length() > 15 || partwords[i].equals("-"))
                partwords[i] = null;
        return partwords;
    }
}
