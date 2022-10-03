# ![logo]() ![title]()
- 切换到 [中文文档]()
### DataTear-HDFS Introduction to related components

  DataTear Some pre implemented components will be stored in the algorithm library, where users can obtain data operation components. The library contains HDFS modules, through which you can directly connect DataTear data read/write business to HDFS. For the use of HDFS, you do not need to manually implement new components!
### Architecture of HDFS components in algorithm library

In the algorithm library, there is a set of HDFS data flow operation components, each of which has two methods to provide data output and data input objects. The following figure shows a basic architecture of HDFS components.
image
The HDFS integration component "Data component" is directly obtained through the algorithm library. Each "Data component" contains its read-write component, which is also the IO layer. You can see various read-write components in the IO layer.

Each class of the IO layer is built in the construction layer. The constructor mode is used here to build classes of each IO layer and integrate them into the "Data component".

### preparation in advance

  The algorithm library contains components for HDFS operations. The components use HDFS APIs, so you need to import HDFS API dependencies first. Note that the versions of each component should be consistent to avoid errors.
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
### Use the algorithm library to convert a local file to DataTear and write it to HDFS
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
        // Instantiate the file system object of HDFS
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://192.168.0.140:8020"), configuration);
        // Get the HDFS operation components through the algorithm library
        // You need to forcibly convert "RW. getDT_UDF_Stream (DT_builtIn_UDF. HDFS_GZIP)" to "HDFSGZIPStream"!
        W_UDF w_udf = (outPath) -> ((HDFSGZIPStream) RW.getDT_UDF_Stream(DT_builtIn_UDF.HDFS_GZIP)).setFileSystem(fileSystem).writeStream(outPath);
        // Load this "W_UDF" into DTMaster
        DTMaster dtMaster = new DTMaster(w_udf)
                .ReadFormat(DataSourceFormat.built_in).WriterFormat(DataOutputFormat.UDT)
                .setIn_FilePath("D:\\test.txt").setOUT_FilePath("hdfs://192.168.0.140/out");

        // start DTMaster
        if (runRW(dtMaster)) {
            System.out.println("ok!!!");
        }
    }

    /**
     * Run an rw component
     */
    public static boolean runRW(RW rw) throws IOException {
        return rw.openStream() && rw.op_Data() && rw.closeStream();
    }
}
```
### Use the algorithm library to read a DataTear file in HDFS
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
        // Instantiate the file system object of HDFS
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://192.168.0.140:8020"), configuration);
        // Get the HDFS operation components through the algorithm library
        R_UDF r_udf = (outPath) -> ((HDFSGZIPStream) RW.getDT_UDF_Stream(DT_builtIn_UDF.HDFS_GZIP)).setFileSystem(fileSystem).readStream(outPath);
        // Load this "R_UDF" into DTMaster
        Reader dtRead = new DTRead(r_udf)
                .setPrimaryCharacteristic(data -> true)
                .setIn_FilePath("hdfs://192.168.0.140/out/NameManager.NDT");

        // start DTRead
        if (runRW(dtRead)) {
            System.out.println("ok!!!");
            System.out.println("The data of file [" + dtRead.getSrcFile() + "] are as follows");
            System.out.println(dtRead.getDataString());
        }
    }

    /**
     * Run an rw component
     */
    public static boolean runRW(RW rw) throws IOException {
        return rw.openStream() && rw.op_Data() && rw.closeStream();
    }
}
```
- 切换到 [中文文档]()