# ![image](https://user-images.githubusercontent.com/113756063/194054780-3c6e7e39-1a93-459f-8f13-74dd14ce5b88.png) ![image](https://user-images.githubusercontent.com/113756063/194055351-81cd7012-515a-43d8-b97c-7aa22fff329b.png)

- Switch to [English document](https://github.com/BeardedManZhao/dataTear/blob/core/KnowledgeDocument/DataTear%20reads%20and%20writes%20in%20database.md)

### 在数据库中构建与获取DataTear

DataTear的算法库中还提供了一套在数据库中的io组件，您可以在数据库中构建或者获取一个DataTear文件，一切实现了JDBC协议的数据库都可以通过这个组件去进行我们的操作

值得注意的是，一般情况是一个库作为一个DataTear文件的存储空间，当然，该组件推荐是在数据库中存储大数据文件的情况下使用，能够有效的改善数据库中表数据量过大的问题

如果数据量不是很大，那么可能会出现“小题大作”的情况，在DataTear中，数据库io组件的架构如下所示
![image](https://user-images.githubusercontent.com/113756063/194054513-8f77fb03-1858-4d0a-8157-d763d50d5e16.png)

### 示例API调用 - DataBaseWriter

接下来我们要演示读取一个csv文件，将其每一行数据进行DataTear，放到数据库中进行存储。

数据输出的组件是从算法库中获取到的，如果您对于算法库不是很了解，您可以先参阅有关算法库的文档。

```
package example.staticResource;

import org.apache.log4j.BasicConfigurator;
import zhao.io.dataTear.atzhaoPublic.W_UDF;
import zhao.io.dataTear.dataOp.DataOutputFormat;
import zhao.io.dataTear.dataOp.DataSourceFormat;
import zhao.io.dataTear.dataOp.dataTearRW.DTMaster;
import zhao.io.dataTear.dataOp.dataTearRW.RW;
import zhao.io.dataTear.dataOp.dataTearStreams.DT_builtIn_UDF;
import zhao.io.dataTear.dataOp.dataTearStreams.dbStream.DataBaseStream;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Test {
    public static void main(String[] args) throws IOException, SQLException {
        BasicConfigurator.configure();
        // 获取到数据库的连接对象
        Connection connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:38243/my_dataBase?", "root", "38243824");
        // 构建出来数据表的字段，注意这里是带有类型的哦！
        String[] writerSchemas = {"name varchar(20)", "sex varchar(20)", "number varchar(20)", "email varchar(20)", "col5 varchar(20)", "col6 varchar(20)"};

        // 从算法库中获取到数据库的读取组件，并实现 W_UDF 组件
        W_UDF w_udf = (outPath) -> ((DataBaseStream) RW.getDT_UDF_Stream(DT_builtIn_UDF.SQLDB_TEXT)).setConnection(connection).setSchemas(writerSchemas).writeStream(outPath);
        // 将我们的 "W_UDF" 装载到 DTMaster
        DTMaster dtMaster = new DTMaster(w_udf)
                .ReadFormat(DataSourceFormat.built_in).WriterFormat(DataOutputFormat.UDT)
                // Set the number of fragments to output, if not set here, the default is 3
                .setFragmentationNum(4)
                // Set the data source, here is a file to read
                .setIn_FilePath("D:\\test.txt")
                // Set the target database
                .setOUT_FilePath("my_dataBase");

        // 启动这个DTMaster
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

### 示例API调用 - DataBaseReader

这里我们继续演示从数据库中读取刚刚写进去的数据，同样读取的组件还是从算法库中获取到的，如果您对于算法库不是很了解，您可以先参阅有关算法库的文档。

```
package example.staticResource;

import org.apache.log4j.BasicConfigurator;
import zhao.io.dataTear.atzhaoPublic.R_UDF;
import zhao.io.dataTear.dataOp.dataTearRW.RW;
import zhao.io.dataTear.dataOp.dataTearRW.Reader;
import zhao.io.dataTear.dataOp.dataTearStreams.DTRead;
import zhao.io.dataTear.dataOp.dataTearStreams.DT_builtIn_UDF;
import zhao.io.dataTear.dataOp.dataTearStreams.dbStream.DataBaseStream;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Test {
    public static void main(String[] args) throws IOException, SQLException {
        BasicConfigurator.configure();
        // 获取到数据库的连接对象
        Connection connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:38243/my_dataBase?", "root", "38243824");
        // 构建出来数据表需要读取的字段，注意这里不需要数据类型了哦！
        String[] readerSchemas = {"name", "sex", "number", "email", "col5", "col6"};
        // 从算法库中获取到数据库的读取组件
        R_UDF r_udf = inPath -> ((DataBaseStream) RW.getDT_UDF_Stream(DT_builtIn_UDF.SQLDB_TEXT)).setConnection(connection).setSchemas(readerSchemas).readStream(inPath);
        // 将我们实现的 "R_UDF" 集成到 DTRead
        Reader dtReader = new DTRead(r_udf)
                // set primary key description
                .setPrimaryCharacteristic(data -> true)
                // Set the data table to be read. When you operate the database, this is usually the NameManager
                .setIn_FilePath("my_dataBase.NameManager");

        // 启动 DTRead
        if (runRW(dtReader)) {
            System.out.println("ok!!!");
            System.out.println(dtReader.getSrcFile() + " The data read result is as follows：");
            System.out.println(dtReader.getDataString());
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

- Switch to [English document](https://github.com/BeardedManZhao/dataTear/blob/core/KnowledgeDocument/DataTear%20reads%20and%20writes%20in%20database.md)
