1.源程序编译方法（在IntelliJ IDEA中）
	生成task1to4.jar：
		File > Project Structure > Add > JAR > Empty，将Name改为task1to4.jar，在Output Layout中：Add > Module Output > LogAnalysis > OK；
		在Output directory中选择jar包地址；
		选中task1to4.jar，将下方的Main Class设置为Task1to4，点击OK；
		Build > Build Artifacts… > Build；
		在选择的地址处可看到名为task1to4.jar的包。

	生成task5.jar：
		File > Project Structure > Delete task1to4.jar > Add > JAR > Empty，将Name改为task5.jar，在Output Layout中：Add > Module Output > LogAnalysis > OK；
		在Output directory中选择jar包地址；
		选中task5.jar，将下方的Main Class设置为Task5，点击OK；
		Build > Build Artifacts… > Build；
		在选择的地址处可看到名为task5.jar的包。

2.jar包运行指令（集群号为2017st15）
	task1to4.jar:
		hadoop jar task1to4.jar /data/task1/JN1_LOG/2015-09-08.log /user/2017st15/out1 /user/2017st15/out2 /user/2017st15/out3 /user/2017st15/out4 

	task5.jar:
		hadoop jar task5.jar /data/task1/JN1_LOG /user/2017st15/out5 
