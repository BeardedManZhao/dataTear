# ![image](https://user-images.githubusercontent.com/113756063/191922682-384a6cd0-684d-4ca0-b442-9352834b036f.png) dataTear

 - dataTear

 拆分成数据碎片去进行数据的管理，在这种格式下，可以实现高效读取，避免不必要的数据读取操作。
  
  - MAVEN dependent
  
    Maven存储库 url:  https://s01.oss.sonatype.org/content/repositories/snapshots
    
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
  
 - 使用示例
 
   下面是针对dataTear文件的读写进行的一个示例，master就是数据输出组件，Reader就是数据读取组件，这俩组件的超接口是同一个，灵活性比较强大！针对参数的设置，可以采取链式，当然您也可以分步进行设置，提供了强大的灵活性。具体使用方式请参阅下面的main函数代码文档!

  - 完整API的调用示例
 
   此处的API调用相对完整，使用到的功能时比较全面的，您可以按照下面的API调用去进行集成开发。
 
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
    
- 最简单的API示例
   如果您对于功能的定制需求没有这么强烈的话，这个API的调用会更加的适合您。它针对必要的参数进行了设置，注意这里的数据输出模式，如果您需要调用自定义的组件或者算法库中的数据组件，您需要将此模式更改为“UDT”。
     
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
    
 - DTMaster组件输出模式
 
 ![image](https://user-images.githubusercontent.com/113756063/191901173-5b01ca42-b2ec-461a-99dc-106a6b711eb7.png)
 - built_in（内置数据输出模式）
  
  内置数据输出组件，相当于LOCAL_TEXT的数据输出模式。在此模式下，您无法加载数据输出组件。但缺点是在此模式_ TEXT模式下，只能使用LOCAL进行数据输出。设置方法如下图所示。
 
![image](https://user-images.githubusercontent.com/113756063/191903087-8d3e70d3-f25e-4a6a-a55d-153a2d7a4c1f.png)

 - UDF（自定义/算法库中的数据输出模式）

  UDF也是一种用户定义的数据输出模式。在此模式下，在实例化DTMaster时必须加载数据输出组件。此数据组件的加载可以从算法库中提取，也可以由您自己直接实现。下面是调用算法库中本地GZIP数据输出组件的示例。
![image](https://user-images.githubusercontent.com/113756063/191902999-d3c19d66-332e-4140-91bf-05d0580fd008.png)


