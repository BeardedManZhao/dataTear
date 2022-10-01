# Customized data io components
- 切换至 [中文文档]()

DataTear supports loading data io components in the constructor. For different needs, you can pass in your component instantiation object here, and DataTear will use the components you provide to perform data io operations.

In Data Tear, the two main classes are DTMaster and DTRead. When these two components are instantiated, you can selectively import data io components. Before this, you need to set the data output mode of DTMaster to "DataOutputFormat. UDT".
image-设置DataOutputFormat.UDT的截图

- Implement data output components

  When the DTMaster data output mode is "DataOutputFormat. UDT", it will automatically use the interface you passed in during construction to obtain data output objects from the interface. For details, see:[W_UDF.java](https://github.com/BeardedManZhao/dataTear/blob/main/src_code1.4.1/src/main/java/zhao/io/dataTear/atzhaoPublic/W_UDF.java)
The formal parameter of the run method in the interface is the data output directory. All you need to do is construct a data flow component through this data output directory. The following is an example.
```
package example.core;

import zhao.io.dataTear.atzhaoPublic.W_UDF;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class MyDataAssembly implements W_UDF {

    @Override
    public OutputStream run(String outPath) throws IOException {
        System.out.println("Custom data output component running.....");
        return new FileOutputStream(outPath);
    }
}
```
- Load custom data output components
  After we have implemented a data output component, we can directly integrate the component into the DTMaster through the construction parameters, so that the DTMaster will use our customized component to write data!!! The following is the sample source code for integrating components into DTMaster.
```
package example.staticResource;

import example.core.MyDataAssembly;
import org.apache.log4j.BasicConfigurator;
import zhao.io.dataTear.dataOp.DataOutputFormat;
import zhao.io.dataTear.dataOp.DataSourceFormat;
import zhao.io.dataTear.dataOp.dataTearRW.DTMaster;

public class Test1 {
    public static void main(String[] args) {

        BasicConfigurator.configure();

        // Integrate components into DTMaster during instantiation
        DTMaster dtMaster = new DTMaster(new MyDataAssembly())
                // Set the data input mode. If it is set to UDF, you need to call the setReader method
                .ReadFormat(DataSourceFormat.built_in)
                // Set the data input mode. If it is set to UDF, you need to pass in a component. We have already passed in a component
                .WriterFormat(DataOutputFormat.UDT)
                // Set the data source of this DTMaster. If you use setReader and the data source is Reader, you do not need to call this method
                .setIn_FilePath("D:\\test.txt")
                // Set the directory where the DTMaster generates DataTear files
                .setOUT_FilePath("C:\\Users\\4\\Desktop\\out");

        // Start DTMaster and judge whether the execution is successful. If it is successful, print a sentence!
        if (dtMaster.openStream() && dtMaster.op_Data() && dtMaster.closeStream()){
            System.out.println("* >>> regular file:" + dtMaster.getIn_FilePath() + " -> DataTear file:" + dtMaster.getOUT_file() + "Output complete!!!");
        }
    }
}
```

- Implement data input components
  
  Our DTRead construction also requires an interface "R_UDF", which has almost the same function as "W_UDF", except that the data input component Reader is extracted from it. In DataTear, the Reader is the source of data and also the data reading class. For more information about "R_UDF", see:[R_UDF.java](https://github.com/BeardedManZhao/dataTear/blob/main/src_code1.4.1/src/main/java/zhao/io/dataTear/atzhaoPublic/R_UDF.java)

Next is the implementation example of our data input component.
```
package example.core;

import zhao.io.dataTear.atzhaoPublic.R_UDF;
import zhao.io.dataTear.dataOp.dataTearRW.Reader;

import java.io.IOException;

public class MyDataAssembly implements R_UDF {

    @Override
    public Reader run(String inPath) throws IOException {
        return new Reader().setIn_FilePath(inPath);
    }
}
```
- Load custom data entry components
  
  After the component is implemented, we can directly provide the component to the DTRead. The specific steps are similar to those of the DTMaster. The example code is as follows
```
package example.staticResource;

import example.core.MyDataAssembly;
import org.apache.log4j.BasicConfigurator;
import zhao.io.dataTear.dataOp.dataTearRW.Reader;
import zhao.io.dataTear.dataOp.dataTearStreams.DTRead;

import java.io.IOException;

public class Test1 {
    public static void main(String[] args) throws IOException {

        BasicConfigurator.configure();

        // Integrate the user-defined data input components into the DTRead when exemplifying the DTRead
        Reader reader = new DTRead(new MyDataAssembly())
                // Set the description of the primary key. Only the data fragment of the primary key meeting the description will be read
                .setPrimaryCharacteristic(data -> true)
                // Set the data input path. Here is the path of the nameManager
                .setIn_FilePath("C:\\Users\\4\\Desktop\\out\\NameManager.NDT");

        // Start DTRead and judge whether the execution is successful. If it is successful, print data!
        if (reader.openStream() && reader.op_Data() && reader.closeStream()) {
            System.out.println("* >>> ------- file：" + reader.getSrcFile() + " The content of is as follows -------");
            System.out.println(reader.getDataString());
        }
    }
}
```
- 切换至 [中文文档]()