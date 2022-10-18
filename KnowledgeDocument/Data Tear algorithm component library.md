# ![Data Tear algorithm component library img](https://user-images.githubusercontent.com/113756063/193436746-c253b493-038d-41e7-82e7-da2ff942e30f.png) ![image](https://user-images.githubusercontent.com/113756063/193436759-34985e17-beb1-44c1-a8cb-a10fbaa23cd1.png)

-

切换至 [中文文档](https://github.com/BeardedManZhao/dataTear/blob/main/KnowledgeDocument/Data%20Tear%20algorithm%20component%20library-Chinese.md)

- **Introduction to Algorithm Library**

  The DataTear algorithm library is a module that helps users simplify the operation steps. It contains many data io
  implementation components. For some common and complex operations, there are implementations here. You can directly
  call the pre-implemented data here. IO components.
- **Component structure in the algorithm library**

  The access to the entire algorithm library is managed by RW. The algorithm library can be accessed through the "
  getDT_UDF_Stream(DT_builtIn_UDF DTfs_streamName)" function in RW. The component class diagram in the algorithm library
  is as follows
  ![algorithm component library Structure](https://user-images.githubusercontent.com/113756063/193436729-5509aefd-701b-46c9-85cb-22d1dc0520fe.png)
- DT_StreamBase

  DT Stream Base is the parent interface of the components in the algorithm library. The two methods provided in this
  interface are used to obtain data input and output components.
- Data component

  Data component is the component layer in the algorithm library. This layer is composed of many DT_StreamBase
  components. Each DT_StreamBase component has different effects. Currently, there are components such as GZIP, which
  support different data io algorithms.
- Data Io Layer

  "Data Io Layer" is the bottom layer implementation of some components, and some components are more complex at the
  bottom, so they generally implement data IO here. All data reading in this layer is the implementation of Reader, and
  all data output is is the implementation of Writer.

- Component building

  "Component building"is the builder implementation class of the components in the data io layer. Our data io layer
  components are all built by the builder.

# Use of Algorithm Libraries

With a general understanding of the structure in the algorithm library, let's start with an example of our use: we want
to compress a Data Tear file into a GZIP Data Tear file.

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

-

切换至 [中文文档](https://github.com/BeardedManZhao/dataTear/blob/main/KnowledgeDocument/Data%20Tear%20algorithm%20component%20library-Chinese.md)
