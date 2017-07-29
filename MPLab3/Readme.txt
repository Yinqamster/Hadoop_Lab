集群账号：2017st15

将InvertedIndex.jar从hadoop用户下拷贝至集群上
scp -r InvertedIndex.jar 2017st15@114.212.190.91:~/

执行jar包
基础：
hadoop jar InvertedIndex.jar InvertedIndex /data/wuxia_novels/. /user/2017st15/output
附加1排序：
hadoop jar InvertedIndex.jar FrequencySort /output/part-r-00000 /user/2017st15/sort-out
附加2：
hadoop jar InvertedIndex.jar TFIDF /output/part-r-00000 /user/2017st15/tf_out


