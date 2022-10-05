# ![image](https://user-images.githubusercontent.com/113756063/194054780-3c6e7e39-1a93-459f-8f13-74dd14ce5b88.png) ![image](https://user-images.githubusercontent.com/113756063/194055266-187c1237-6dc4-4154-8d35-ef55012c847b.png)

- 切换至 [中文文档](https://github.com/BeardedManZhao/dataTear/blob/core/KnowledgeDocument/DataTear%20reads%20and%20writes%20in%20database-Chinese.md)

### Build and get Data Tears in the database

Data Tear's algorithm library also provides a set of io components in the database. You can build or obtain a Data Tear
file in the database. All databases that implement the JDBC protocol can use this component to perform our operations.

It is worth noting that in general, a library is used as the storage space for a Data Tear file. Of course, this
component is recommended to be used in the case of storing large data files in the database, which can effectively
improve the problem of excessive table data in the database.

If the amount of data is not very large, there may be a situation of "wasting resources". In Data Tear, the schema of
the database io component is as follows
![image](https://user-images.githubusercontent.com/113756063/194054513-8f77fb03-1858-4d0a-8157-d763d50d5e16.png)

### Example API call - DataBaseWriter

Next, we will demonstrate reading a csv file, perform Data Tear on each line of data, and store it in the database.

The components of the data output are obtained from the algorithm library. If you are not familiar with the algorithm
library, you can refer to the documentation about the algorithm library first.

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
        // get database connection
        Connection connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:38243/my_dataBase?", "root", "38243824");
        // Build table fields in database
        String[] writerSchemas = {"name varchar(20)", "sex varchar(20)", "number varchar(20)", "email varchar(20)", "col5 varchar(20)", "col6 varchar(20)"};

        // Get the read and write components of the database from the algorithm library
        W_UDF w_udf = (outPath) -> ((DataBaseStream) RW.getDT_UDF_Stream(DT_builtIn_UDF.SQLDB_TEXT)).setConnection(connection).setSchemas(writerSchemas).writeStream(outPath);
        // Load this "W_UDF" into DTMaster
        DTMaster dtMaster = new DTMaster(w_udf)
                .ReadFormat(DataSourceFormat.built_in).WriterFormat(DataOutputFormat.UDT)
                // Set the number of fragments to output, if not set here, the default is 3
                .setFragmentationNum(4)
                // Set the data source, here is a file to read
                .setIn_FilePath("D:\\test.txt")
                // Set the target database
                .setOUT_FilePath("my_dataBase");

        // start DTMaster
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

### Example API call - DataBaseReader

Here we continue to demonstrate reading the data just written in from the database, and the read components are also
obtained from the algorithm library. If you are not familiar with the algorithm library, you can refer to the document
about the algorithm library first.

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
        // get database connection
        Connection connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:38243/my_dataBase?", "root", "38243824");
        // Build table fields in database
        String[] readerSchemas = {"name", "sex", "number", "email", "col5", "col6"};
        // Get the read and write components of the database from the algorithm library
        R_UDF r_udf = inPath -> ((DataBaseStream) RW.getDT_UDF_Stream(DT_builtIn_UDF.SQLDB_TEXT)).setConnection(connection).setSchemas(readerSchemas).readStream(inPath);
        // Load this "R_UDF" into DTRead
        Reader dtReader = new DTRead(r_udf)
                // set primary key description
                .setPrimaryCharacteristic(data -> true)
                // Set the data table to be read. When you operate the database, this is usually the NameManager
                .setIn_FilePath("my_dataBase.NameManager");

        // start DTRead
        if (runRW(dtReader)) {
            System.out.println("ok!!!");
            System.out.println(dtReader.getSrcFile() + " The data read result is as follows：");
            System.out.println(dtReader.getDataString());
        }
    }

    /**
     * run a rw component
     */
    public static boolean runRW(RW rw) throws IOException {
        return rw.openStream() && rw.op_Data() && rw.closeStream();
    }
}
```

- 切换至 [中文文档](https://github.com/BeardedManZhao/dataTear/blob/core/KnowledgeDocument/DataTear%20reads%20and%20writes%20in%20database-Chinese.md)
