package zhao.io.dataTear.dataOp.dataTearRW;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zhao.io.dataTear.atzhaoPublic.Builder;
import zhao.io.dataTear.atzhaoPublic.Priority;
import zhao.io.dataTear.atzhaoPublic.W_UDF;
import zhao.io.dataTear.dataOp.DataOutputFormat;
import zhao.io.dataTear.dataOp.DataSourceFormat;
import zhao.io.ex.ZHAOLackOfInformation;

import java.io.File;

/**
 * @author 赵凌宇
 * @version 1.4.2
 * <p>
 * 写DataTear数据实现类，读取数据，按照DataTear格式规范转换构建输出文件
 * <p>
 * 采用以自身为建造者设计模式进行实例化，省去建造者类的冗余
 * <p>
 * 是数据撕裂的主要模块，通过该模块可以构建出对应的DataTear目录
 * <p>
 * <p>
 * Write the DataTear data implementation class, read the data, convert and construct the output file according to the DataTear format specification, and instantiate it with the design mode of the builder itself, eliminating the redundancy of the builder class is the main module of data tearing. Through this module, you can Build the corresponding DataTear directory
 * <p>
 * API调用示例：
 * <p>
 * RW rw = new DTMaster(OutPath -》 RW.getDT_UDF_Stream(DT_builtIn_UDF.LOCAL_TEXT).writeStream(s)) // 实现的UDF，这里为了看起来简洁，使用了lambda的样式展示
 * <p>
 * .ReadFormat(DataSourceFormat.built_in) // 设置数据输入模式为内置
 * <p>
 * .WriterFormat(DataOutputFormat.UDT) // 设置数据输出模式为自定义，这里自定义将会调用实现的UDF方法
 * <p>
 * .setUseSynchronization(true) // 是否使用同步写数据，等待数据输出完成再结束
 * <p>
 * .setIn_FilePath("/.../.....") // 设置被读取的文件路径
 * <p>
 * .setOUT_FilePath("/.../....") // 设置DataTear数据输出到哪个目录
 * <p>
 * .setSplitrex(",") // 设置数据输入的列分隔符
 * <p>
 * .setOutSplit(",") // 设置数据输出的列分隔符
 * <p>
 * .setPrimaryNum(1) // 设置数据表中的主键索引，该索引列的数据将会被作为nameManager的一部分
 * <p>
 * .setFragmentationNum(2); // 设置输出多少个数据碎片
 * <p>
 * rw.openStream(); // 打开数据流
 * <p>
 * rw.op_Data(); // 进行数据操作
 * <p>
 * rw.closeStream(); // 关闭数据流
 */
public class DTMasterBuilder implements Builder<DTMaster> {
    final static Logger LOGGER = LoggerFactory.getLogger("DataTear_Core_Builder");
    final W_UDF udf;
    final boolean useNonparametricStructure;
    int threshold = 2;
    String In_FilePath;
    File In_File;
    String OUT_FilePath;
    File OUT_file;
    DataSourceFormat dataSourceFormat = DataSourceFormat.built_in;
    DataOutputFormat dataOutputFormat = DataOutputFormat.built_in;
    Reader reader;
    String splitRex = "\\s+";
    String outSplit = ",";
    int primaryNum = 0;
    int FragmentationNum = 4;
    String charset;
    boolean useSynchronization = true;

    public DTMasterBuilder() {
        this.udf = null;
        WriterFormat(DataOutputFormat.built_in);
        LOGGER.warn("您使用了无参构造方式获取到DTMaster，请注意，在此模式下您不需要进行数据输出模式的设置了");
        this.useNonparametricStructure = true;
    }

    public DTMasterBuilder(W_UDF udf) {
        this.udf = udf;
        useNonparametricStructure = false;
    }

    /**
     * 调用此方法设置写数据的异步或同步的并发模式 默认是true，如果您追求效率，那么您可以选择异步，如果您追求代码中的安全性，那么您可以选择同步。
     * <p>
     * Call this method to set the asynchronous or synchronous concurrency mode of writing data. The default is true
     * <p>
     * If you are after efficiency then you can choose async, if you are after safety in your code then you can choose synchronous.
     *
     * @param useSynchronization 是否使用同步写数据的布尔值 true 代表使用同步
     *                           <p>
     *                           Whether to use synchronization to write data boolean true means use synchronization
     * @return 链
     */
    public DTMasterBuilder setUseSynchronization(boolean useSynchronization) {
        this.useSynchronization = useSynchronization;
        return this;
    }

    /**
     * 该组件需要使用到的编码集 不一定会生效，但是当需要使用到编码集的时候，会按照您指定的编码集类进行输出字符转码！
     * 为了在下一次读取的时候不会出错误，您可以选择性的使用本方法
     * <p>
     * 注意：如果您不指定，需要使用字符集的时候将默认使用utf-8
     * <p>
     * The code set that this component needs to use will not necessarily take effect, but when the code set needs to be used, the output character transcoding will be performed according to the code set class you specified!
     * <p>
     * In order to avoid errors in the next reading, you can use this method selectively.
     * <p>
     * Note: If you don't specify it, utf-8 will be used by default when you need to use the character set.
     *
     * @param charset 您输出的数据是什么编码集
     * @return 链
     */
    public DTMasterBuilder setCharset(String charset) {
        this.charset = charset;
        return this;
    }

    /**
     * 通过自定义数据输入组件的方式进行数据的读取，如果使用了这个方法，那么之前的输入设置将会被忽略，框架会通过Reader提取数据，然后在输出
     * <p>
     * 平台中有内置的数据Reader组件，如果您有自定义数据输入的需求，您可以选择重写Reader类
     * <p>
     * <p>
     * The data is read by customizing the data input component. If this method is used, the previous input settings will be ignored, the framework will extract the data through the Reader
     * <p>
     * there is a built-in data Reader component in the output platform. If You have a need for custom data input, you can choose to override the Reader class.
     *
     * @param reader 需要使用的读数据组件
     * @return 链
     * @see Reader
     */
    @Priority("1")
    public DTMasterBuilder setReader(Reader reader) {
        if (this.dataSourceFormat == DataSourceFormat.UDT) {
            this.reader = reader;
            this.In_File = reader.getIn_File();
            this.In_FilePath = reader.getIn_FilePath();
        } else if (this.dataSourceFormat == DataSourceFormat.built_in) {
            String s = "您设置的输入模式为[" + this.dataSourceFormat + "] 此模式不允许设置Reader类，请您调用 setInPath() 进行数据源的设置。";
            LOGGER.error(s, new ZHAOLackOfInformation(s));
        }
        return this;
    }


    /**
     * 设置数据输出的后的数据分隔符号，当数据输出之后，其结构也是一张表的，在最终DT的构建中是有列分隔符的。
     * <p>
     * Set the data separator after the data output. When the data is output, its structure is also a table, and there are column separators in the final DT construction.
     *
     * @param outSplit 输出的数据表中的符号 注意不是正则哦  Note that the symbols in the output data table are not regular
     * @return 链
     */
    public DTMasterBuilder setOutSplit(String outSplit) {
        this.outSplit = outSplit;
        return this;
    }

    /**
     * 设置需要输出的DT数据碎片数量，底层会通过标记数值，对该值取余，将数据输出到对应的文件数据碎片中。
     * <p>
     * Set the number of DT data fragments to be output, the bottom layer will mark the value, take the remainder of the value, and output the data to the corresponding file data fragment.
     * <p>
     * 从1.42 版本开始，该方法只能接收 2的n次方个数据碎片的处理
     * <p>
     * Starting from version 1.42, this method can only accept the processing of data fragments to the nth power of 2
     *
     * @param fragmentationNum 您需要将文件拆分成多少数据碎片  How many data fragments do you need to split the file into
     * @return 链式
     */
    public DTMasterBuilder setFragmentationNum(int fragmentationNum) {
        int threshold = fragmentationNum & (fragmentationNum - 1);
        if (threshold == 0) {
            this.FragmentationNum = fragmentationNum;
            this.threshold = (int) Math.ceil(Math.sqrt(fragmentationNum));
        } else {
            throw new RuntimeException("很抱歉，处于效率的考虑，您只能设置2^n次方个数据碎片的输出。");
        }
        return this;
    }

    /**
     * 设置需要用来构建索引的字段在列中的序号，从0开始排序，可以理解这个就是一个主键，它会作为索引和数据碎片进行关联
     * <p>
     * Set the serial number of the field that needs to be used to build the index in the column, starting from 0. It can be understood that this is a primary key, which will be used as an index to associate with data fragments
     *
     * @param primaryNum 设置主键在字段中的索引位置 从0开始生效 提取到的对应位置的主键数据将会作为非常重要的桥梁
     *                   <p>
     *                   Set the index position of the primary key in the field to take effect from 0. The extracted primary key data of the corresponding position will serve as a very important bridge
     * @return 链式
     */
    public DTMasterBuilder setPrimaryNum(int primaryNum) {
        this.primaryNum = primaryNum;
        return this;
    }

    /**
     * 设置数据输入的列分割正则，会按照该正则将一份数据的字段分割出来。
     * <p>
     * Set the column division regularity of data input, and the fields of a piece of data will be divided according to the regularity.
     *
     * @param splitRex 数据切分符  data splitter
     * @return 链
     */
    public DTMasterBuilder setSplitrex(String splitRex) {
        this.splitRex = splitRex;
        return this;
    }


    /**
     * 设置数据的输入模式，获取数据，注意 如果该选项为UDT，那么必须要使用setReader方法设置数据输入组件。
     * <p>
     * Set the data input mode and get the data. Note that if this option is UDT, you must use the setReader method to set the data input component.
     *
     * @param dataSourceFormat 是否使用自定义的数据输入组件 默认为使用自定义组件  Whether to use a custom data input component The default is to use a custom component
     * @return 链
     * @see Reader
     */
    public DTMasterBuilder ReadFormat(DataSourceFormat dataSourceFormat) {
        this.dataSourceFormat = dataSourceFormat;
        return this;
    }

    /**
     * 设置数据的输出模式，默认就是UDT模式
     * <p>
     * Set the output mode of the data, the default is UDT mode
     *
     * @param dataSinkFormat 是否使用自定义的数据输入组件 默认为是
     *                       <p>
     *                       Whether to use a custom data input component default is yes
     * @return 链
     */
    public DTMasterBuilder WriterFormat(DataOutputFormat dataSinkFormat) {
        if (this.useNonparametricStructure) {
            String s = "您使用了无参方式构造[" + this.getClass().getName() + "]组件，意味着您不需要进行的数据模式设置，请检查您的API调用。";
            LOGGER.error(s, new ZHAOLackOfInformation(s));
        } else {
            this.dataOutputFormat = dataSinkFormat;
        }
        return this;
    }

    /**
     * 通过文件对象的形式，设置需要读取的文件，setIn_File 与 setIn_Path 与 setInputStream 一般来说要设置其中的一个，具体是否需要设置，还需要看实现的子类们是如何获取
     * <p>
     * Set the file to be read in the form of a file object. Generally, setIn_File, setIn_Path and setInputStream need to set one of them. Whether it needs to be set depends on how the subclasses of the implementation obtain it.
     *
     * @param in_FilePath 被读取的文件路径 file path to be read
     * @return 链
     */
    public DTMasterBuilder setIn_FilePath(String in_FilePath) {
        if (this.dataSourceFormat == DataSourceFormat.built_in) {
            try {
                In_FilePath = in_FilePath;
                In_File = new File(in_FilePath);
            } catch (NullPointerException n) {
                throw new ZHAOLackOfInformation("您为DTMaster设置的信息不全哦！请将输入路径设置一下！", LOGGER);
            }
        } else if (this.dataSourceFormat == DataSourceFormat.UDT) {
            String s = "您设置的输入模式为[" + this.dataSourceFormat + "] 此模式不允许设置数据输入路径，而是设置Reader类，请您调用 setReader() 进行数据源的设置。";
            LOGGER.error(s, new ZHAOLackOfInformation(s));
        }
        return this;
    }

    /**
     * 该方法是对于DT输出路径的描述，需要指定的是一个目录，当启动opData方法的时候，框架将会自动的将该目录下面的文件全部刷新，然后构建DT目录。
     * <p>
     * This method is a description of the DT output path. A directory needs to be specified. When the op Data method is started, the framework will automatically refresh all the files under the directory, and then build the DT directory.
     *
     * @param OUT_FilePath DT路径构建目录  DT path build directory
     * @return 链
     */
    public DTMasterBuilder setOUT_FilePath(String OUT_FilePath) {
        try {
            this.OUT_FilePath = OUT_FilePath;
            OUT_file = new File(OUT_FilePath);
        } catch (NullPointerException n) {
            throw new ZHAOLackOfInformation("您为DTMaster设置的信息不全哦！请将输出路径设置一下！", LOGGER);
        }
        return this;
    }


    /**
     * 该方法是对DT输出路径的描述，需要指定的是一个目录的File类对象，当启动opData的死后，将会从File中提取输出路径。
     * <p>
     * This method is a description of the DT output path. It needs to specify a File class object of a directory. When the death of op Data is started, the output path will be extracted from the File.
     *
     * @param OUT_file 输出路径文件对象
     * @return 链
     */
    public DTMasterBuilder setOUT_file(File OUT_file) {
        try {
            this.OUT_file = OUT_file;
            OUT_FilePath = OUT_file.getAbsolutePath();
        } catch (NullPointerException n) {
            throw new ZHAOLackOfInformation("您为DTMaster设置的信息不全哦！请将输出路径对象设置一下！", LOGGER);
        }
        return this;
    }

    @Override
    public DTMaster create() {
        return null;
    }
}
