# ![image](https://user-images.githubusercontent.com/113756063/193436880-7a0ee80e-dc44-485d-863e-d9f2133dc79f.png) ![Customized data io components title-c](https://user-images.githubusercontent.com/113756063/193436868-dadd6230-25ae-4251-aed5-d7359ee0a7cd.png)

- Switch
  to [English document](https://github.com/BeardedManZhao/dataTear/blob/main/KnowledgeDocument/Customized%20data%20io%20components.md)

Data Tear 支持在构造函数中装载数据io组件，针对不同的需求，您可以在此传入您的组件实例化对象，Data Tear 将会使用您提供的组件去进行数据的io操作。

在Data Tear 中，主要的两个类就是DTMaster 以及 DTRead，这两个组件在示例化的时候您可以选择性的传入数据io组件，在这之前，您需要将DTMaster的数据输出模式设置为"DataOutputFormat.UDT"。
![image](https://user-images.githubusercontent.com/113756063/193394129-1dbf3983-5e8d-461b-82ec-398c6860a2b1.png)

- 实现数据输出组件

  DTMaster数据输出模式为"DataOutputFormat.UDT"
  的时候，就会自动的使用您在构造时传入的接口，从接口中获取数据输出对象，具体可以参阅：[W_UDF.java](https://github.com/BeardedManZhao/dataTear/blob/main/src_code/src/main/java/zhao/io/dataTear/atzhaoPublic/W_UDF.java)
  接口中的run方法的形参就是数据的输出目录，您需要做的就是通过这个数据输出目录，构造出来一个数据流组件，下面是有关的示例。

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

- 装载自定义的数据输出组件

  当我们实现好一个数据输出组件之后，可以直接将组件通过构造参数集成到DTMaster，这样DTMaster就会使用我们自定义的组件去写数据啦！！！下面是将组件集成到DTMaster的示例源码

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

- 实现数据输入组件

  我们的DTRead构造也是需要一个接口"R_UDF"，该接口的作用与"W_UDF"几乎一致，只不过从中提取出来的是数据输入组件Reader，在DataTear中，Reader就是数据的来源，也是数据读取类。有关"R_UDF"
  的详细信息，请参阅:[R_UDF.java](https://github.com/BeardedManZhao/dataTear/blob/main/src_code/src/main/java/zhao/io/dataTear/atzhaoPublic/R_UDF.java)

接下来就是我们数据输入组件的实现示例

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

- 装载自定义数据输入组件

  当组件实现好之后，我们直接将组件提供给DTRead就好，具体步骤与DTMaster差不多，示例代码如下所示

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

- Switch
  to [English document](https://github.com/BeardedManZhao/dataTear/blob/main/KnowledgeDocument/Customized%20data%20io%20components.md)
