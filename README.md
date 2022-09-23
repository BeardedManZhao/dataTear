# dataTear
 - dataTear
  
  Split into data blocks,In this format, efficient reading can be realized,Avoid unnecessary data reading operations.
  
  - MAVEN dependent

        <dependency>
            <groupId>io.github.BeardedManZhao</groupId>
            <artifactId>dataTear</artifactId>
            <version>1.4-SNAPSHOT</version>
        </dependency>
  
 - Example of use
 
  Chinese：下面是针对dataTear文件的读写进行的一个示例，master就是数据输出组件，Reader就是数据读取组件，这俩组件的超接口是同一个，灵活性比较强大！具体使用方式请参阅下面的main函数代码文档!
  
  
  English：The following is an example of reading and writing the dataTear file. The master is the data output component, and the Reader is the data reading component. The hyperinterfaces of these two components are the same, which is more flexible! Please refer to the following main function code document for specific usage!

  	public static void main(String[] args) throws IOException, SQLException {
  
        BasicConfigurator.configure();
        Date date = new Date();
        // 通过RW 将Master实例化
        RW rw = new DTMaster(s -> RW.getDT_UDF_Stream(DT_builtIn_UDF.LOCAL_TEXT).writeStream(s)) // 实例化时，可以将外界的数据组件对接到本类中，也可以直接调用RW接口从算法库中提取数据组件
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
        RW reader = new DTRead(s -> RW.getDT_UDF_Stream(DT_builtIn_UDF.LOCAL_TEXT).readStream(s)) // 实例化时，可以将外界的数据组件对接到本类中，也可以直接调用RW接口从算法库中提取数据组件
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
