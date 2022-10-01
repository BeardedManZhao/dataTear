# ![image](https://user-images.githubusercontent.com/113756063/191922682-384a6cd0-684d-4ca0-b442-9352834b036f.png) dataTear

 - 切换至：[中文文档](https://github.com/BeardedManZhao/dataTear/blob/main/README-Chinese.md)
 - knowledge base
<a href="知识库文档uri">
 <img src = "知识库卡片uri"/>
</a>
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
            // Instantiate "Master" through RW， When instantiating, external data components can be integrated into this class, or the RW interface can be directly called to extract data components from the algorithm library
            DTMaster dtMaster = new DTMaster(s -> RW.getDT_UDF_Stream(DT_builtIn_UDF.LOCAL_GZIP).writeStream(s)) 
                    .ReadFormat(DataSourceFormat.built_in).WriterFormat(DataOutputFormat.UDT) // Set data input and output mode
                    .setUseSynchronization(true) // Whether to use synchronous data writing and wait for data output to complete
                    .setIn_FilePath("D:\\InternetInformation.txt") // Set the read file path
                    .setOUT_FilePath("C:\\Users\\4\\Desktop\\out") // Set the directory to which DataTear data is output
                    .setSplitrex(",") // Set the column separator for data input
                    .setOutSplit(",") // Set the column separator for data output
                    .setPrimaryNum(0) // Set the primary key index in the data table. The data of the index column will be used as part of the nameManager
                    .setFragmentationNum(2); // Set the number of output data fragments
            // Running components
            runRW(dtMaster);
    
            System.err.println("ok !  Time consuming to write data：" + (new Date().getTime() - date.getTime()) + "");
    
            /*TODO 数据组件分割 */
    
            Date date2 = new Date();
            // Instantiate "DTRead" through RW， When instantiating, external data components can be integrated into this class, or the RW interface can be directly called to extract data components from the algorithm library
            Reader dtRead = new DTRead(s -> RW.getDT_UDF_Stream(DT_builtIn_UDF.LOCAL_GZIP).readStream(s))
                    .setPrimaryCharacteristic(data -> true) // Set the data primary key description. The data fragment of the primary key meeting this condition will be read
                    .setUseMultithreading(true) // Set whether to use synchronous read
                    .setMaxOutTimeMS(10000) // Set the maximum timeout (ms) for data reading. If the timeout is exceeded, data reading will be stopped immediately
                    .setIn_FilePath("C:\\Users\\4\\Desktop\\out\\NameManager.NDT"); // Set the read NameManager path
            // Running components
            runRW(dtRead);
    
            System.err.println("ok !  Time consuming to read data：" + (new Date().getTime() - date2.getTime()) + "millisecond");
            System.err.println("source file：" + dtRead.getSrcFile() + "\tCreation time：" + new Date(dtRead.getCreateDateMS()).toLocaleString());
            System.err.println("Number of data rows：" + dtRead.getDataString().split("\n").length);
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
        // Configure Data Output Class
        DTMaster dtMaster = new DTMaster(null)
                .WriterFormat(DataOutputFormat.built_in) // be careful!!! If UDF is not set here, data will be automatically written in "LOCAL_TEXT" mode
                .setPrimaryNum(0)
                .setIn_FilePath("C:\\Users\\4\\Desktop\\mathematicalModeling\\Attached documents\\test.txt") // Set the path of the converted file
                .setOUT_FilePath("C:\\Users\\4\\Desktop\\mathematicalModeling\\out") // Set the storage path of NM and other files after conversion
                .setSplitrex("\\s+");
        dtMaster.openStream();
        dtMaster.op_Data();
        dtMaster.closeStream();

        // Configure Data Reading Class
        Reader reader = new DTRead(InPath -> RW.getDT_UDF_Stream(DT_builtIn_UDF.LOCAL_TEXT).readStream(InPath))
                .setPrimaryCharacteristic((data) -> true)
                .setIn_FilePath("C:\\Users\\4\\Desktop\\mathematicalModeling\\out\\NameManager.NDT"); // Set the NM path of the read file
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
