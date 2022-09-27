# ![image](https://user-images.githubusercontent.com/113756063/191922682-384a6cd0-684d-4ca0-b442-9352834b036f.png) dataTear

 - 切换至：[中文文档](https://github.com/BeardedManZhao/dataTear/blob/main/README-Chinese.md)
 - dataTear

 Split into data fragments for data management. In this format, efficient reading can be achieved to avoid unnecessary data reading operations..
  
  - MAVEN dependent
  
    Maven repository url:  https://s01.oss.sonatype.org/content/repositories/snapshots
    
        <repositories>
            <repository>
                <id>a</id>
                <name>sonatype</name>
                <url>https://s01.oss.sonatype.org/content/repositories/snapshots</url>
            </repository>
        </repositories>
    
        <dependencies>
            <dependency>
                <groupId>io.github.BeardedManZhao</groupId>
                <artifactId>dataTear</artifactId>
                <version>1.4-SNAPSHOT</version>
            </dependency>
        </dependencies>
  
 - Example of use
 
    The following is an example of reading and writing the dataTear file. The master is the data output component, and the Reader is the data reading component. The hyperinterfaces of these two components are the same, which is more flexible! The parameters can be set in chain mode. Of course, you can also set them step by step, providing strong flexibility. Please refer to the following main function code document for specific usage!
 
- Full API Example
 
    The API calls here are relatively complete, and the functions used are relatively comprehensive. You can use the following API calls for integrated development.
 
    public static void main(String[] args) throws IOException {
            BasicConfigurator.configure();
            Date date = new Date();
            // 通过RW 将Master实例化
            DTMaster dtMaster = new DTMaster(s -> RW.getDT_UDF_Stream(DT_builtIn_UDF.LOCAL_GZIP).writeStream(s)) // 实例化时，可以将外界的数据组件对接到本类中，也可以直接调用RW接口从算法库中提取数据组件
                    .ReadFormat(DataSourceFormat.built_in).WriterFormat(DataOutputFormat.UDT) // 设置数据输入与输出模式
                    .setUseSynchronization(true) // 是否使用同步写数据，等待数据输出完成再结束
                    .setIn_FilePath("D:\\互联网信息.txt") // 设置被读取的文件路径
                    .setOUT_FilePath("C:\\Users\\4\\Desktop\\out") // 设置DataTear数据输出到哪个目录
                    .setSplitrex(",") // 设置数据输入的列分隔符
                    .setOutSplit(",") // 设置数据输出的列分隔符
                    .setPrimaryNum(0) // 设置数据表中的主键索引，该索引列的数据将会被作为nameManager的一部分
                    .setFragmentationNum(2); // 设置输出多少个数据碎片
            // 运行组件
            runRW(dtMaster);
    
            System.err.println("ok !  写数据耗时：" + (new Date().getTime() - date.getTime()) + "毫秒");
    
            /*TODO 数据组件分割 */
    
            Date date2 = new Date();
            // 通过RW将Reader实例化
            Reader dtRead = new DTRead(s -> RW.getDT_UDF_Stream(DT_builtIn_UDF.LOCAL_GZIP).readStream(s)) // 实例化时，可以将外界的数据组件对接到本类中，也可以直接调用RW接口从算法库中提取数据组件
                    .setPrimaryCharacteristic(data -> true) // 设置数据主键描述，满足该条件的主键所在数据碎片将会被读取
                    .setUseMultithreading(true) // 设置是否使用同步读取
                    .setMaxOutTimeMS(10000) // 设置数据读取最大超时时间（毫秒），超出时间将会立刻停止数据的读取
                    .setIn_FilePath("C:\\Users\\4\\Desktop\\out\\NameManager.NDT"); // 设置被读取的NameManager路径
            // 运行组件
            runRW(dtRead);
    
            System.err.println("ok !  读数据耗时：" + (new Date().getTime() - date2.getTime()) + "毫秒");
            System.err.println("源文件：" + dtRead.getSrcFile() + "\t创建时间：" + new Date(dtRead.getCreateDateMS()).toLocaleString());
            System.err.println("数据行数：" + dtRead.getDataString().split("\n").length);
        }
    
        /**
         * 运行一个rw组件
         */
        public static boolean runRW(RW rw) throws IOException {
            return rw.openStream() && rw.op_Data() && rw.closeStream();
        }
    
- The simplest API example
     
   If your customization requirements for functions are not so strong, this API call will be more suitable for you. It sets the necessary parameters. Note the data output mode here. If you need to call custom components or data components in the algorithm library, you need to change this mode to "UDT".

      public static void main(String[] args) throws IOException {
        BasicConfigurator.configure();
        // 配置数据输出类
        DTMaster dtMaster = new DTMaster(null)
                .WriterFormat(DataOutputFormat.built_in) // 注意！！！这里如果不设置UDF，那么将会自动的使用 LOCAL_TEXT 模式写数据
                .setPrimaryNum(0)
                .setIn_FilePath("C:\\Users\\4\\Desktop\\数学建模\\附带文件\\test.txt") // 设置被转换文件的路径
                .setOUT_FilePath("C:\\Users\\4\\Desktop\\数学建模\\out") // 设置转换之后的NM等文件的存储路径
                .setSplitrex("\\s+");
        dtMaster.openStream();
        dtMaster.op_Data();
        dtMaster.closeStream();

        // 配置数据读取类
        Reader reader = new DTRead(InPath -> RW.getDT_UDF_Stream(DT_builtIn_UDF.LOCAL_TEXT).readStream(InPath))
                .setPrimaryCharacteristic((data) -> true)
                .setIn_FilePath("C:\\Users\\4\\Desktop\\数学建模\\out\\NameManager.NDT"); // 设置被读取文件的NM路径
        reader.openStream();
        reader.op_Data();
        reader.closeStream();

        System.out.println(reader.getDataString());
      }
    
 - DTMaster component output mode
 
 ![image](https://user-images.githubusercontent.com/113756063/191901173-5b01ca42-b2ec-461a-99dc-106a6b711eb7.png)
 - built_in
  
  The mode of using the built-in data output component is equivalent to the data output component of "LOCAL_TEXT". In this mode, you do not need to load the data output component. However, the disadvantage is that when using the built-in data component output mode, you can only use "LOCAL_TEXT" for data output. The setting method is shown in the following figure.
 
![image](https://user-images.githubusercontent.com/113756063/191903087-8d3e70d3-f25e-4a6a-a55d-153a2d7a4c1f.png)

 - UDF

 UDF is also a user-defined data output mode. In this mode, you must load a data output component when instantiating the DTMaster. The loading of this data component can be extracted from the algorithm library or directly implemented by yourself. The following is an example of calling the local GZIP data output component in the algorithm library.
![image](https://user-images.githubusercontent.com/113756063/191902999-d3c19d66-332e-4140-91bf-05d0580fd008.png)

 - 切换至：[中文文档](https://github.com/BeardedManZhao/dataTear/blob/main/README-Chinese.md)
