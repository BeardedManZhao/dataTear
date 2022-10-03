# ![logo]() ![title]()
- Switch to [English Document]()
### DataTear-HDFS相关组件介绍

  DataTear预先实现好的一些组件都会存储在算法库中，用户可以在库中进行数据操作组件的获取，库中包含了HDFS的模块，您可以通过这些模块直接将DataTear数据读写业务对接到HDFS中，针对HDFS的使用，您不需要去手动实现新组件啦!
### 算法库中HDFS组件的架构

  在算法库中，有着一套HDFS的数据流操作组件，它们每一个组件中都有两个方法，分别提供数据输出与数据输入的对象，下图中展示的就是HDFS组件的一个基本架构。
  image
  其中HDFS的集成组件"Data component"就是通过算法库直接获取到的，每一个"Data component"中都包含者它的读写组件，这个读写组件也就是io层，在io层中可以看到有·各种各样的读写组件。
  
每一个io层的类，都是在构建层被构建出来的，这里采用的是建造者模式，将每一个io层的类构建出来，集成到"Data component"中。
### 前期准备

  注意哦！算法库中包含针对HDFS操作的组件，组件使用到了HDFS的API，因此您需要先将HDFS的API依赖导入进来，注意各个组件之间的版本要一致，避免出现错误。
```
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>3.3.2</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-hdfs</artifactId>
            <version>3.3.2</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
            <version>3.3.2</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-hdfs-client</artifactId>
            <version>3.3.2</version>
        </dependency>
```
### 使用算法库将一个本地文件转化到DataTear，并写到HDFS中
```
package example.staticResource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.BasicConfigurator;
import zhao.io.dataTear.atzhaoPublic.W_UDF;
import zhao.io.dataTear.dataOp.DataOutputFormat;
import zhao.io.dataTear.dataOp.DataSourceFormat;
import zhao.io.dataTear.dataOp.dataTearRW.DTMaster;
import zhao.io.dataTear.dataOp.dataTearRW.RW;
import zhao.io.dataTear.dataOp.dataTearStreams.DT_builtIn_UDF;
import zhao.io.dataTear.dataOp.dataTearStreams.hdfsStream.HDFSGZIPStream;

import java.io.IOException;
import java.net.URI;

public class Test {
    public static void main(String[] args) throws IOException {
        BasicConfigurator.configure();
        Configuration configuration = new Configuration();
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        // 实例化HDFS的文件系统对象
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://192.168.0.140:8020"), configuration);
        // 通过算法库获取HDFS操作组件
        // 您需要将“RW.getDT_UDF_Stream(DT_builtIn_UDF.HDFS_GZIP)”转换为“HDFSGZIPStream”！
        W_UDF w_udf = (outPath) -> ((HDFSGZIPStream) RW.getDT_UDF_Stream(DT_builtIn_UDF.HDFS_GZIP)).setFileSystem(fileSystem).writeStream(outPath);
        // 将此“W_UDF”加载到DTMaster
        DTMaster dtMaster = new DTMaster(w_udf)
                .ReadFormat(DataSourceFormat.built_in).WriterFormat(DataOutputFormat.UDT)
                .setIn_FilePath("D:\\test.txt").setOUT_FilePath("hdfs://192.168.0.140/out");

        // 启动 DTMaster
        if (runRW(dtMaster)) {
            System.out.println("ok!!!");
        }
    }

    /**
     * 运行一个rw组件
     */
    public static boolean runRW(RW rw) throws IOException {
        return rw.openStream() && rw.op_Data() && rw.closeStream();
    }
}
```
### 使用算法库读取HDFS中的一个DataTear文件
```
package example.staticResource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.BasicConfigurator;
import zhao.io.dataTear.atzhaoPublic.R_UDF;
import zhao.io.dataTear.dataOp.dataTearRW.RW;
import zhao.io.dataTear.dataOp.dataTearRW.Reader;
import zhao.io.dataTear.dataOp.dataTearStreams.DTRead;
import zhao.io.dataTear.dataOp.dataTearStreams.DT_builtIn_UDF;
import zhao.io.dataTear.dataOp.dataTearStreams.hdfsStream.HDFSGZIPStream;

import java.io.IOException;
import java.net.URI;

public class Test2 {
    public static void main(String[] args) throws IOException {
        BasicConfigurator.configure();
        Configuration configuration = new Configuration();
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        // 实例化HDFS的文件系统对象
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://192.168.0.140:8020"), configuration);
        // 通过算法库获取HDFS操作组件
        // 您需要将“RW.getDT_UDF_Stream(DT_builtIn_UDF.HDFS_GZIP)”转换为“HDFSGZIPStream”！
        R_UDF r_udf = (outPath) -> ((HDFSGZIPStream) RW.getDT_UDF_Stream(DT_builtIn_UDF.HDFS_GZIP)).setFileSystem(fileSystem).readStream(outPath);
        // 将此“R_UDF”加载到DTMaster
        Reader dtRead = new DTRead(r_udf)
                .setPrimaryCharacteristic(data -> true)
                .setIn_FilePath("hdfs://192.168.0.140/out/NameManager.NDT");

        // 启动 DTRead
        if (runRW(dtRead)) {
            System.out.println("ok!!!");
            System.out.println("The data of file [" + dtRead.getSrcFile() + "] are as follows");
            System.out.println(dtRead.getDataString());
        }
    }

    /**
     * 运行一个rw组件
     */
    public static boolean runRW(RW rw) throws IOException {
        return rw.openStream() && rw.op_Data() && rw.closeStream();
    }
}
```
- Switch to [English Document]()