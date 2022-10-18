# ![Data Tear algorithm component library img](https://user-images.githubusercontent.com/113756063/193436746-c253b493-038d-41e7-82e7-da2ff942e30f.png) ![image](https://user-images.githubusercontent.com/113756063/193436796-c762335e-6f8f-4f53-b0fd-b05e3fd8f6d1.png)

- Switch
  to [English document](https://github.com/BeardedManZhao/dataTear/blob/main/KnowledgeDocument/Data%20Tear%20algorithm%20component%20library.md)
- 算法库介绍

  DataTear 算法库是一个帮助使用者简化操作步骤的模块，其中包含许多的数据io实现组件，针对一些比较常用且复杂的操作，在这里都有实现，您可以直接在此处调用预先实现好的数据IO组件。
- 算法库中的组件结构

  整个算法库的访问被RW所托管，通过RW中的"getDT_UDF_Stream(DT_builtIn_UDF DTfs_streamName)"函数能够访问算法库，算法库中的组件类图如下所示
  ![algorithm component library Structure](https://user-images.githubusercontent.com/113756063/193436729-5509aefd-701b-46c9-85cb-22d1dc0520fe.png)
- DT_StreamBase

  DT_StreamBase是算法库中的组件的父类接口，该接口中提供的两个方法是用来获取数据输入与输出组件的。
- Data component

  Data component是算法库中的组件层，该层中是很多DT_StreamBase组成的，每一个DT_StreamBase组件都有不同的效果，目前可以看到有GZIP等组件，支持不同的数据io算法。

- Data Io Layer

  Data Io Layer是部分组件的底层实现，有的组件底层较为复杂，因此它们一般会在这里进行数据IO的实现，这一层中的所有数据读取皆是Reader的实现，所有数据输出皆是Writer的实现。

- Component building

  Component building是数据io层中组件的建造者实现类，我们数据io层组件皆是采用建造者构建出来的。

# **算法库的使用**

大致了解了下算法库中的构造，下面开始我们的一个使用示例：我们希望将一个DataTear的文件压缩为GZIP的DataTear的文件。

```java
package example.staticResource;

import org.apache.log4j.BasicConfigurator;
import zhao.io.dataTear.dataOp.DataOutputFormat;
import zhao.io.dataTear.dataOp.DataSourceFormat;
import zhao.io.dataTear.dataOp.dataTearRW.DTMaster;
import zhao.io.dataTear.dataOp.dataTearRW.RW;
import zhao.io.dataTear.dataOp.dataTearRW.Reader;
import zhao.io.dataTear.dataOp.dataTearStreams.DTRead;
import zhao.io.dataTear.dataOp.dataTearStreams.DT_builtIn_UDF;

import java.io.IOException;

public class Test1 {
    public static void main(String[] args) throws IOException {

        BasicConfigurator.configure();

        String textDataTearFileInPath = "C:\\Users\\4\\Desktop\\out\\NameManager.NDT";
        String GzipDataTearFileOutPath = "C:\\Users\\4\\Desktop\\out1";

        // Construct a DataTear data reading class to read the DataTear file of Text
        Reader reader = new DTRead(inPath -> RW.getDT_UDF_Stream(DT_builtIn_UDF.LOCAL_TEXT).readStream(inPath))
                .setPrimaryCharacteristic(data -> true)
                .setIn_FilePath(textDataTearFileInPath);

        // Construct a data output class to output the data obtained from "Reader" with GZIP algorithm
        DTMaster dtMaster = new DTMaster(outPath -> RW.getDT_UDF_Stream(DT_builtIn_UDF.LOCAL_GZIP).writeStream(outPath))
                /* Set the data read/write mode to UDF.
                 * This is because data is read from DTRead and data is written using the GZIP algorithm component of the algorithm library.
                 * "DTRead" belongs to your instantiation, and "GZIP" belongs to the algorithm library, which is not a built-in component of DTMaster.
                 */
                .ReadFormat(DataSourceFormat.UDT).WriterFormat(DataOutputFormat.UDT)
                // Set the Reader class as the data source
                .setReader(reader)
                .setOUT_FilePath(GzipDataTearFileOutPath);
        // Start running DTMaster component
        if (runRW(dtMaster)) {
            System.out.println("ok!!!");
        }

    }

    /**
     * start an RW component
     */
    public static boolean runRW(RW rw) throws IOException {
        return rw.openStream() && rw.op_Data() && rw.closeStream();
    }
}
```

- Switch
  to [English document](https://github.com/BeardedManZhao/dataTear/blob/main/KnowledgeDocument/Data%20Tear%20algorithm%20component%20library.md)
