1.Դ������뷽������IntelliJ IDEA�У�
	����task1to4.jar��
		File > Project Structure > Add > JAR > Empty����Name��Ϊtask1to4.jar����Output Layout�У�Add > Module Output > LogAnalysis > OK��
		��Output directory��ѡ��jar����ַ��
		ѡ��task1to4.jar�����·���Main Class����ΪTask1to4�����OK��
		Build > Build Artifacts�� > Build��
		��ѡ��ĵ�ַ���ɿ�����Ϊtask1to4.jar�İ���

	����task5.jar��
		File > Project Structure > Delete task1to4.jar > Add > JAR > Empty����Name��Ϊtask5.jar����Output Layout�У�Add > Module Output > LogAnalysis > OK��
		��Output directory��ѡ��jar����ַ��
		ѡ��task5.jar�����·���Main Class����ΪTask5�����OK��
		Build > Build Artifacts�� > Build��
		��ѡ��ĵ�ַ���ɿ�����Ϊtask5.jar�İ���

2.jar������ָ���Ⱥ��Ϊ2017st15��
	task1to4.jar:
		hadoop jar task1to4.jar /data/task1/JN1_LOG/2015-09-08.log /user/2017st15/out1 /user/2017st15/out2 /user/2017st15/out3 /user/2017st15/out4 

	task5.jar:
		hadoop jar task5.jar /data/task1/JN1_LOG /user/2017st15/out5 
