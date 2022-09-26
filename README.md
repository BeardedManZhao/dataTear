# ![image](https://user-images.githubusercontent.com/113756063/191922682-384a6cd0-684d-4ca0-b442-9352834b036f.png) dataTear

 - dataTear

  Split into data blocks,In this format, efficient reading can be realized,Avoid unnecessary data reading operations.
  
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
 
  Chinese：下面是针对dataTear文件的读写进行的一个示例，master就是数据输出组件，Reader就是数据读取组件，这俩组件的超接口是同一个，灵活性比较强大！具体使用方式请参阅下面的main函数代码文档!
  
  
  English：The following is an example of reading and writing the dataTear file. The master is the data output component, and the Reader is the data reading component. The hyperinterfaces of these two components are the same, which is more flexible! Please refer to the following main function code document for specific usage!
  - Full API Example
 
 API calls here are relatively complete
 
    public static void main(String[] args) throws IOException, SQLException {
        BasicConfigurator.configure();
        Date date = new Date();
        // 通过RW 将Master实例化
        RW rw = new DTMaster(s -> RW.getDT_UDF_Stream(DT_builtIn_UDF.LOCAL_GZIP).writeStream(s)) // 实例化时，可以将外界的数据组件对接到本类中，也可以直接调用RW接口从算法库中提取数据组件
                .ReadFormat(DataSourceFormat.built_in).WriterFormat(DataOutputFormat.UDT) // 设置数据输入与输出模式
                .setUseSynchronization(true) // 是否使用同步写数据，等待数据输出完成再结束
                .setIn_FilePath("E:\\FileSave\\School_Work\\test.txt") // 设置被读取的文件路径
                .setOUT_FilePath("C:\\Users\\4\\Desktop\\out") // 设置DataTear数据输出到哪个目录
                .setSplitrex(",") // 设置数据输入的列分隔符
                .setOutSplit(",") // 设置数据输出的列分隔符
                .setPrimaryNum(1) // 设置数据表中的主键索引，该索引列的数据将会被作为nameManager的一部分
                .setFragmentationNum(2); // 设置输出多少个数据碎片

        rw.openStream(); // 打开数据流
        rw.op_Data(); // 进行数据操作
        rw.closeStream(); // 关闭数据流
        System.err.println("ok !  写数据耗时：" + (new Date().getTime() - date.getTime()) + "毫秒");

        /*TODO 数据组件分割 */

        Date date2 = new Date();
        // 通过RW将Reader实例化
        RW reader = new DTRead(s -> RW.getDT_UDF_Stream(DT_builtIn_UDF.LOCAL_GZIP).readStream(s)) // 实例化时，可以将外界的数据组件对接到本类中，也可以直接调用RW接口从算法库中提取数据组件
                .setPrimaryCharacteristic(data -> true) // 设置数据主键描述，满足该条件的主键所在数据碎片将会被读取
                .setUseMultithreading(true) // 设置是否使用同步读取
                .setMaxOutTimeMS(10000) // 设置数据读取最大超时时间（毫秒），超出时间将会立刻停止数据的读取
                .setIn_FilePath("C:\\Users\\4\\Desktop\\out\\NameManager.NDT"); // 设置被读取的NameManager路径
        reader.openStream();
        reader.op_Data();
        reader.closeStream();

        System.err.println("ok !  读数据耗时：" + (new Date().getTime() - date2.getTime()) + "毫秒");
        System.err.println("源文件：" + reader.getSrcFile() + "\t创建时间：" + new Date(reader.getCreateDateMS()).toLocaleString());
        System.err.println("数据行数：" + reader.getDataString().split("\n").length);
      }
- The simplest API example
     
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
  
  Built in data output component, equivalent to data output mode of LOCAL_TEXT,In this mode, you can not load the data output component, but the disadvantage is that you can only use LOCAL in this mode_ TEXT mode for data output,The setting method is shown in the following figure.
 
![image](https://user-images.githubusercontent.com/113756063/191903087-8d3e70d3-f25e-4a6a-a55d-153a2d7a4c1f.png)

 - UDF

 UDF is also a user-defined data output mode. In this mode, you must load a data output component when instantiating the DTMaster. The loading of this data component can be extracted from the algorithm library or directly implemented by yourself. The following is an example of calling the local GZIP data output component in the algorithm library.
![image](https://user-images.githubusercontent.com/113756063/191902999-d3c19d66-332e-4140-91bf-05d0580fd008.png)


