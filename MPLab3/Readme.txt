��Ⱥ�˺ţ�2017st15

��InvertedIndex.jar��hadoop�û��¿�������Ⱥ��
scp -r InvertedIndex.jar 2017st15@114.212.190.91:~/

ִ��jar��
������
hadoop jar InvertedIndex.jar InvertedIndex /data/wuxia_novels/. /user/2017st15/output
����1����
hadoop jar InvertedIndex.jar FrequencySort /output/part-r-00000 /user/2017st15/sort-out
����2��
hadoop jar InvertedIndex.jar TFIDF /output/part-r-00000 /user/2017st15/tf_out


