import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by yq on 7/22/17.
 */
public class Predict {
    private static HashMap<String, Integer>[][] dateList;
    //predict value of data 09-22
    private static HashMap<String, Integer>[] predict22 = new HashMap[24];
    //occur times of url
    private static HashMap<String, Integer>[] urlTimes = new HashMap[24];
    //average occur time of url
    private static HashMap<String, Double>[] averageTimes = new HashMap[24];
    //varience occur time of url
    private static HashMap<String, Double>[] varienceTimes = new HashMap[24];
    //max occur time of url
    private static HashMap<String, Integer>[] maxTime = new HashMap[24];
    //min occur time of url
    private static HashMap<String, Integer>[] minTime = new HashMap[24];
    private static HashMap<String, Double>[] changeRate = new HashMap[24];
    private static int hourNum;
    private static int fileNum;
    private static void init(int hN, int fN, HashMap<String, Integer>[][] dL) {
        hourNum = hN;
        fileNum = fN;
        dateList = dL;
        for (int i = 0; i < hourNum; i++) {
            predict22[i] = new HashMap<String, Integer>();
            urlTimes[i] = new HashMap<String, Integer>();
            averageTimes[i] = new HashMap<String, Double>();
            varienceTimes[i] = new HashMap<String, Double>();
            maxTime[i] = new HashMap<String, Integer>();
            minTime[i] = new HashMap<String, Integer>();
            changeRate[i] = new HashMap<String, Double>();
        }
    }


    public static double getWeight(int date) {
        double weight=0.0;
        if (date != 0) {
            weight = Math.pow(0.5, 14 - date);
        }
        else if(date ==0) {
            weight = Math.pow(0.5,13);
        }
        return weight;
    }

    public static HashMap<String,Integer>[]  predict(int hourNum, int fileNum, HashMap<String,Integer>dateList[][]) {
        init(hourNum, fileNum, dateList);
        for (int j = 0; j < hourNum; j++) {
            for (int i = 0; i < fileNum; i++){
                double weight = getWeight(i);
                Iterator it = dateList[i][j].keySet().iterator();
                while(it.hasNext()) {
                    String url = (String) it.next();
                    int times = dateList[i][j].get(url);
                    times *= weight;
                    if (predict22[j].containsKey(url)) {
                        int sum = predict22[j].get(url);
                        sum+=times;
                        predict22[j].put(url,sum);
                    }
                    else {
                        predict22[j].put(url,times);
                    }
                }
            }
            Iterator it =predict22[j].keySet().iterator();
            while (it.hasNext()) {
                String url = (String) it.next();
                int time = predict22[j].get(url);
                    predict22[j].put(url, time);

            }
        }
        return predict22;
    }


    //compute average of url
    private static void computeAverage() {
        for (int j = 0; j < hourNum; j++) {
            for (int i = 0; i < fileNum; i++){
                //            double weight = getWeight(i);
                Iterator it = dateList[i][j].keySet().iterator();
                while(it.hasNext()) {
                    String url = (String) it.next();
                    int times = dateList[i][j].get(url);
                    //                times *= weight;
                    if (predict22[j].containsKey(url)) {
                        int sum = predict22[j].get(url);
                        sum+=times;
                        predict22[j].put(url,sum);
                        int occurTime = urlTimes[j].get(url);
                        occurTime++;
                        urlTimes[j].put(url,occurTime);
                    }
                    else {
                        predict22[j].put(url,times);
                        urlTimes[j].put(url,1);
                    }
                }
            }
            Iterator it =predict22[j].keySet().iterator();
            while (it.hasNext()) {
                String url = (String) it.next();
                int sum = predict22[j].get(url);
                int times = urlTimes[j].get(url);
                double average = (double)sum / (double)times;
                averageTimes[j].put(url, average);

            }
        }
    }

    //compute varience, max, min of url
    private static void computeVolatility() {
        for (int j = 0; j < hourNum; j++) {
            for(int i = 0; i < fileNum; i++) {
                Iterator it = dateList[i][j].keySet().iterator();
                while(it.hasNext()){
                    String url = (String)it.next();
                    int times = dateList[i][j].get(url);
                    if(varienceTimes[j].containsKey(url)){
                        double sum = varienceTimes[j].get(url);
                        sum += Math.pow(times-averageTimes[j].get(url),2);
                        varienceTimes[j].put(url,sum);
                    }
                    else {
                        varienceTimes[j].put(url,Math.pow(times-averageTimes[j].get(url),2));
                    }

                    if(maxTime[j].containsKey(url)) {
                        int max = maxTime[j].get(url);
                        if (times > max) {
                            maxTime[j].put(url,times);
                        }
                    }
                    else {
                        maxTime[j].put(url,times);
                    }

                    if(minTime[j].containsKey(url)) {
                        int min = minTime[j].get(url);
                        if (times < min) {
                            minTime[j].put(url,times);
                        }
                    }
                    else {
                        minTime[j].put(url,times);
                    }
                }
            }
            Iterator it =varienceTimes[j].keySet().iterator();
            while (it.hasNext()) {
                String url = (String) it.next();
                double sum = varienceTimes[j].get(url);
                int times = urlTimes[j].get(url);
                double aveVar = (double)sum / (double) times;
                varienceTimes[j].put(url, aveVar);

            }

            Iterator ch = maxTime[j].keySet().iterator();
            while(ch.hasNext()) {
                String url = (String) ch.next();
                int max = maxTime[j].get(url);
                int min = minTime[j].get(url);
                double rate = (double)(max - min) / (double)min;
                changeRate[j].put(url,rate);
            }

        }
    }

    //predict url at 09-22
    private static void predictDate22() {
        for (int i = 0; i < hourNum; i++) {
            predict22[i].clear();
            Iterator it = varienceTimes[i].keySet().iterator();
            while (it.hasNext()) {
                String url = (String) it.next();
                double sum = varienceTimes[i].get(url);
                double rate = changeRate[i].get(url);
                if (sum > 1000000 && rate > 1) {
                    int index = 13;
                    while(!dateList[index][i].containsKey(url)) {
                        index--;
                        if(index == -1) break;
                    }
                    if (index != -1) {
                        predict22[i].put(url, dateList[index][i].get(url));
                        //                   outflag[i].put(url,index);
                    }
                    else {
                        int ave = new Double(averageTimes[i].get(url)).intValue();
                        double delt = averageTimes[i].get(url)-ave;
                        predict22[i].put(url,delt > 0.5 ?  ave+1 : ave);
                        //                  outflag[i].put(url,-1);
                    }
                }
                else {
                    int ave = new Double(averageTimes[i].get(url)).intValue();
                    double delt = averageTimes[i].get(url)-ave;
                    predict22[i].put(url,delt > 0.5 ?  ave+1 : ave);
                    //            outflag[i].put(url,-2);
                }
            }
        }
    }


    public static HashMap<String,Integer>[] predictNew(int hourNum, int fileNum, HashMap<String,Integer>dateList[][]) {
        init(hourNum,fileNum,dateList);
        computeAverage();
        computeVolatility();
        predictDate22();

        return predict22;
    }
}
