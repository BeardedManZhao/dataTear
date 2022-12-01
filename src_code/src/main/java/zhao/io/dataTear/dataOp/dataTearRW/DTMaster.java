package zhao.io.dataTear.dataOp.dataTearRW;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zhao.io.dataTear.atzhaoPublic.Priority;
import zhao.io.dataTear.atzhaoPublic.W_UDF;
import zhao.io.dataTear.dataContainer.DataFragmentation;
import zhao.io.dataTear.dataContainer.RWTable;
import zhao.io.dataTear.dataOp.DataOutputFormat;
import zhao.io.dataTear.dataOp.DataSourceFormat;
import zhao.io.dataTear.dataOp.dataTearStreams.DTWrite;
import zhao.io.ex.ZHAOLackOfInformation;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

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
public class DTMaster implements RW {
    private final Logger logger = LoggerFactory.getLogger("DataTear_Core");
    private final W_UDF udf;
    private final boolean useNonparametricStructure;
    private int threshold = 2;
    private String In_FilePath;
    private File In_File;
    private String OUT_FilePath;
    private File OUT_file;
    private BufferedInputStream inputStream;
    private DataSourceFormat dataSourceFormat = DataSourceFormat.built_in;
    private DataOutputFormat dataOutputFormat = DataOutputFormat.built_in;
    private Reader reader;
    private String splitRex = "\\s+";
    private String outSplit = ",";
    private int primaryNum = 0;
    private int FragmentationNum = 4;
    private String charset;
    private boolean useSynchronization = true;

    /**
     * 使用内置的数据输出组件构造一个DTMaster
     * <p>
     * Construct a DT Master using built-in data output components
     *
     * @apiNote 该无参构建DTMaster的方式从1.4.2版本开始支持，在之前的版本您想要使用此功能，需要通过"DTMaster(W_UDF udf)"进行构造，调用了该方法之后，您将不允许设置该组件的"WriterFormat"
     * <p>
     * This method of constructing DTMaster without parameters is supported from version 1.4.2. If you want to use this function in previous versions, you need to construct it through "DTMaster(W_UDF udf)". After calling this method, you will not be allowed to set this function. The component's "WriterFormat"
     */
    public DTMaster() {
        this.udf = null;
        WriterFormat(DataOutputFormat.built_in);
        logger.warn("您使用了无参构造方式获取到DTMaster，请注意，在此模式下您不需要进行数据输出模式的设置了");
        this.useNonparametricStructure = true;
    }

    /**
     * 构建DataTear写数据组件 是DataTear写数据的组件，该组件中采用链式设置各个参数，您可以根据实际情况进行调用。
     * <p>
     * Build DataTear data writing component It is the data writing component of DataTear. This component adopts chained setting of various parameters, and you can call it according to the actual situation.
     *
     * @param udf W_UDF接口实现类 其中的run应返回一个写数据的实现, 您可以使用Lambda的方式将数据组件从此接口中返回，本类将会去提取组件。
     *            <p>
     *            W_UDF interface implementation class The run should return an implementation of writing data. You can use Lambda to return data components from this interface, and this class will extract components.
     */
    public DTMaster(W_UDF udf) {
        this.udf = udf;
        useNonparametricStructure = false;
    }

    /**
     * @return 是否使用同步的方式写数据，默认是true
     * <p>
     * Whether to use synchronous way to write data, the default is true.
     * @apiNote 每一个数据碎片的输出是并发式的，效率很高，针对并发输出的异同步，您可以根据实际情况进行设置。
     * <p>
     * The output of each data fragment is concurrent, with high efficiency. You can set the asynchronous output of the concurrent output according to the actual situation.
     */
    public boolean isUseSynchronization() {
        return useSynchronization;
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
    public DTMaster setUseSynchronization(boolean useSynchronization) {
        this.useSynchronization = useSynchronization;
        return this;
    }

    /**
     * 获取输出数据的字符集编码类型
     * <p>
     * Get the character set encoding type of the output data
     *
     * @return 输出DT数据使用的字符集
     * <p>
     * Character set used for output DT data
     */
    public String getCharset() {
        return charset;
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
    public DTMaster setCharset(String charset) {
        this.charset = charset;
        return this;
    }


    /**
     * 自定义输出组件方法，强制实现，其中会自动使用该方法对接Writer接口，并将该方法运用，如果您使用的是自定义输出模式的话，如果您没有使用自定义的输出模式，那么该方法将不会被调用。
     * <p>
     * Custom output component method, mandatory implementation, which will automatically use this method to connect to the Writer interface, and use this method, if you are using a custom output mode, if you do not use a custom output mode, then this method not be called.
     *
     * @param OUT_FilePath 转换结果输出路径  Conversion result output path
     * @return 通过实现的方案获取到的输出流对象  The output stream object obtained through the implemented scheme
     * @throws IOException 自定义的流打开失败  Custom stream open failed
     */
    protected OutputStream UDTOutputStream(String OUT_FilePath) throws IOException {
        return this.udf.run(OUT_FilePath);
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
    public DTMaster setReader(Reader reader) {
        if (this.dataSourceFormat == DataSourceFormat.UDT) {
            this.reader = reader;
            this.In_File = reader.getIn_File();
            this.In_FilePath = reader.getIn_FilePath();
        } else if (this.dataSourceFormat == DataSourceFormat.built_in) {
            String s = "您设置的输入模式为[" + this.dataSourceFormat + "] 此模式不允许设置Reader类，请您调用 setInPath() 进行数据源的设置。";
            logger.error(s, new ZHAOLackOfInformation(s));
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
    public DTMaster setOutSplit(String outSplit) {
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
    public DTMaster setFragmentationNum(int fragmentationNum) {
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
    public DTMaster setPrimaryNum(int primaryNum) {
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
    public DTMaster setSplitrex(String splitRex) {
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
    public DTMaster ReadFormat(DataSourceFormat dataSourceFormat) {
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
    public DTMaster WriterFormat(DataOutputFormat dataSinkFormat) {
        if (this.useNonparametricStructure) {
            String s = "您使用了无参方式构造[" + this.getClass().getName() + "]组件，意味着您不需要进行的数据模式设置，请检查您的API调用。";
            logger.error(s, new ZHAOLackOfInformation(s));
        } else {
            this.dataOutputFormat = dataSinkFormat;
        }
        return this;
    }

    public String getIn_FilePath() {
        return In_FilePath;
    }

    /**
     * 通过文件对象的形式，设置需要读取的文件，setIn_File 与 setIn_Path 与 setInputStream 一般来说要设置其中的一个，具体是否需要设置，还需要看实现的子类们是如何获取
     * <p>
     * Set the file to be read in the form of a file object. Generally, setIn_File, setIn_Path and setInputStream need to set one of them. Whether it needs to be set depends on how the subclasses of the implementation obtain it.
     *
     * @param in_FilePath 被读取的文件路径 file path to be read
     * @return 链
     */
    public DTMaster setIn_FilePath(String in_FilePath) {
        if (this.dataSourceFormat == DataSourceFormat.built_in) {
            try {
                In_FilePath = in_FilePath;
                In_File = new File(in_FilePath);
            } catch (NullPointerException n) {
                throw new ZHAOLackOfInformation("您为DTMaster设置的信息不全哦！请将输入路径设置一下！", logger);
            }
        } else if (this.dataSourceFormat == DataSourceFormat.UDT) {
            String s = "您设置的输入模式为[" + this.dataSourceFormat + "] 此模式不允许设置数据输入路径，而是设置Reader类，请您调用 setReader() 进行数据源的设置。";
            logger.error(s, new ZHAOLackOfInformation(s));
        }
        return this;
    }


    public String getOUT_FilePath() {
        return OUT_FilePath;
    }

    /**
     * 该方法是对于DT输出路径的描述，需要指定的是一个目录，当启动opData方法的时候，框架将会自动的将该目录下面的文件全部刷新，然后构建DT目录。
     * <p>
     * This method is a description of the DT output path. A directory needs to be specified. When the op Data method is started, the framework will automatically refresh all the files under the directory, and then build the DT directory.
     *
     * @param OUT_FilePath DT路径构建目录  DT path build directory
     * @return 链
     */
    public DTMaster setOUT_FilePath(String OUT_FilePath) {
        try {
            this.OUT_FilePath = OUT_FilePath;
            OUT_file = new File(OUT_FilePath);
        } catch (NullPointerException n) {
            throw new ZHAOLackOfInformation("您为DTMaster设置的信息不全哦！请将输出路径设置一下！", logger);
        }
        return this;
    }

    public File getOUT_file() {
        return OUT_file;
    }

    /**
     * 该方法是对DT输出路径的描述，需要指定的是一个目录的File类对象，当启动opData的死后，将会从File中提取输出路径。
     * <p>
     * This method is a description of the DT output path. It needs to specify a File class object of a directory. When the death of op Data is started, the output path will be extracted from the File.
     *
     * @param OUT_file 输出路径文件对象
     * @return 链
     */
    public DTMaster setOUT_file(File OUT_file) {
        try {
            this.OUT_file = OUT_file;
            OUT_FilePath = OUT_file.getAbsolutePath();
        } catch (NullPointerException n) {
            throw new ZHAOLackOfInformation("您为DTMaster设置的信息不全哦！请将输出路径对象设置一下！", logger);
        }
        return this;
    }

    /**
     * 内置的加载数据碎片的方法,该方法会被用来转换为RWTable需要的格式，最终会被添加的RWTable中。
     * <p>
     * The built-in method of loading data fragments, this method will be used to convert the format required by the RW Table, and will eventually be added to the RW Table.
     *
     * @param readData 需要被转换成表数组的数据String  The data String that needs to be converted into a table array
     * @return 数据表的二维数组  2D array of data tables
     */
    private ArrayList<String[]> loadData(String readData) {
        return loadData(readData.split("\n"));
    }

    private ArrayList<String[]> loadData(String[] lines) {
        final ArrayList<String[]> linesList = new ArrayList<>(lines.length);
        for (int i = 0, j = lines.length - 1; i < j; ++i, --j) {
            linesList.add(lines[i].split(splitRex));
            linesList.add(lines[j].split(splitRex));
        }
        return linesList;
    }

    /**
     * 打开数据流，同时返回状态，这个数据流的获取会先判断是否是自定义的数据输入模式，如果是的话，将会打开reader的数据流。
     * <p>
     * Open the data stream and return the status at the same time. The acquisition of this data stream will first determine whether it is a custom data input mode. If so, the reader's data stream will be opened.
     *
     * @return 是否成功打开数据流  Whether the data stream was successfully opened
     */
    @Override
    public boolean openStream() {
        logger.info("准备初始化数据流。");
        try {
            if (dataSourceFormat == DataSourceFormat.UDT) {
                logger.info("以自定义组件进行数据输入流的打开。进入到自定义组件[" + reader + "]函数栈。");
                return reader.openStream();
            } else {
                logger.info("以内置组件的方式打开数据流。");
                if (reader != null) {
                    logger.warn("您的输入模式以及数据输入的组件设置不一致！设置的时候使用了自定义的Read组件，但是您又设置数据加载模式为内置，这会导致程序可能不会去启动您的Read类。");
                }
                inputStream = new BufferedInputStream((new FileInputStream(In_File)));
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace(System.err);
            return false;
        } catch (NullPointerException n) {
            logger.error("发生了空指针异常，具体原因请看堆栈信息。", n);
            return false;
        }
    }

    /**
     * 操作数据的方法，这里是写数据，如果您是使用的Reader，那么这里将会提取Reader的结果数据，然后将数据写出去。
     * <p>
     * The method of manipulating data, here is writing data, if you are using Reader, here will extract the result data of Reader, and then write the data out.
     *
     * @return 写数据事件成功或失败。  write data succeeds or fails.
     */
    @Override
    public boolean op_Data() {
        byte[] datas = new byte[(int) In_File.length()];
        RWTable<String> rwTable = new RWTable<>(this.threshold);
        rwTable.setPrimaryKeyNum(primaryNum).setFragmentationNum(FragmentationNum);
        try {
            if (dataSourceFormat == DataSourceFormat.built_in) {
                StringBuilder stringBuilder = new StringBuilder();
                logger.info("使用内置组件的方式开始载入缓冲区数据。");
                int offset;
                do {
                    byte[] bufferByte = new byte[datas.length];
                    offset = inputStream.read(bufferByte);
                    stringBuilder.append(new String(bufferByte).trim());
                } while (offset != -1);
                datas = stringBuilder.toString().getBytes(this.charset == null ? "utf-8" : this.charset);
            } else {
                reader.op_Data();
                datas = reader.getDataArray();
                logger.info("使用自定义组件开始载入缓冲区数据。");
            }
            try {
                rwTable.putAllData(loadData(new String(datas).split("\n")));
                logger.info("数据加载完成！开始进行DataTear的格式转换，构建rwTable");
            } catch (ArrayIndexOutOfBoundsException e) {
                logger.error("您设置的primaryKey索引不存在于数据中，您的数据列数可能小于您设置的primaryIndex序号，请您检查数据表结构,并重新 setPrimaryNum()。错误索引：" + e.getLocalizedMessage(), e);
                return false;
            }
            try {
                writerNameManagerAndData(rwTable);
            } catch (ArrayIndexOutOfBoundsException e) {
                logger.error("您设置的数据输出流在启动的时候，出现错误了哦！。" + e);
                e.printStackTrace(System.err);
                return false;
            }
            return true;
        } catch (IOException e) {
            if (dataSourceFormat == DataSourceFormat.built_in) {
                logger.error("数据输出流异常：原因：" + e, e);
            } else {
                logger.error("UDF输出流异常：原因：" + e, e);
            }
            return false;
        }
    }

    /**
     * 将RWTable数据以DataTear的格式持久化到磁盘，同时将与之关联的数据碎片路径补全。
     * <p>
     * Persist RW Table data to disk in Data Tear format, and complete the associated data fragment path.
     *
     * @param rwTable 关联的数据碎片对象  Associated Data Fragment Object
     * @throws IOException 构造NameManager失败
     */
    protected void writerNameManagerAndData(RWTable<String> rwTable) throws IOException {
        long startTimeMS = new Date().getTime();
        final StringBuilder FragmentationPaths = new StringBuilder(rwTable.getFragmentationNum() + 0b10000);
        boolean isNotUdf = dataOutputFormat == DataOutputFormat.built_in;
        final CountDownLatch countDownLatch = new CountDownLatch(this.FragmentationNum + 1);
        // 通过Fragmentation编号绘制路径，同时向输出对应路径的数据碎片数据，最后标记压缩格式
        final StringBuilder writeLoad = new StringBuilder("【");
        final AtomicInteger real = new AtomicInteger();
        final AtomicInteger okNum = new AtomicInteger();
        for (int FragmentationN = 0; FragmentationN < rwTable.getFragmentationNum(); FragmentationN++) {
            Writer FragmentationOutputStream;
            final int finalFragmentationN = FragmentationN;
            try {
                final String FragmentationPath = OUT_FilePath + "/Fragmentation-" + finalFragmentationN + ".DT";
                FragmentationOutputStream = isNotUdf ? DTWrite.bulider().setPath(FragmentationPath).create() : new DTWrite(UDTOutputStream(FragmentationPath), FragmentationPath);
                FragmentationPaths.append(FragmentationOutputStream.getPath()).append("&");
                new Thread(() -> {
                    try {
                        writerFragmentation(FragmentationOutputStream, finalFragmentationN, rwTable);
                        countDownLatch.countDown();
                        okNum.addAndGet(1);
                        writeLoad.append("#");
                    } catch (IOException e) {
                        logger.error("Fragmentation[" + finalFragmentationN + "] 输出异常，原因：" + e);
                        e.printStackTrace(System.err);
                        writeLoad.append("X");
                        real.addAndGet(1);
                    }
                }, "DataWriterThread[" + finalFragmentationN + "]").start();
            } catch (ArrayIndexOutOfBoundsException | NullPointerException e) {
                writeLoad.append("X");
                real.addAndGet(1);
            }
        }
        logger.info("开始绘制NameManager，并构建索引。");
        final String NameManager_Path = OUT_FilePath + "/NameManager.NDT";
        DTWrite NameManagerOutStream = isNotUdf ? DTWrite.bulider().setPath(NameManager_Path).create() : new DTWrite(UDTOutputStream(NameManager_Path), NameManager_Path);
        new Thread(() -> {
            String nameManagerData = (rwTable.superString() + FragmentationPaths + "\r\n" + "srcFile = " + In_FilePath + "\n");
            try {
                NameManagerOutStream.write(nameManagerData.getBytes(this.charset == null ? "utf-8" : this.charset));
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("NameManager绘制失败：" + e);
            } finally {
                try {
                    NameManagerOutStream.flush();
                } catch (IOException ignored) {
                }
                try {
                    NameManagerOutStream.close();
                } catch (IOException ignored) {
                }
            }
            countDownLatch.countDown();
        }).start();
        if (isUseSynchronization()) {
            try {
                logger.info("请稍等，正在同步并发写数据中...");
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
        }
        if (isUseSynchronization()) {
            logger.info("NameManager以及Fragmentation 绘制完成····················共耗时【" + (new Date().getTime() - startTimeMS) + "】毫妙。");
            logger.info("数据碎片输出情况：" + writeLoad + "】·······················总数据碎片【" + rwTable.getFragmentationNum() + "】\t丢失X【" + real.get() + "】\t成功#【" + okNum.get() + "】");
        } else {
            logger.warn("请注意，异步方式写数据将依赖于主进程生命周期，请确保主进程不会退出，否则将导致数据输出不全。");
            logger.info("NameManager绘制完成·····························共耗时【" + (new Date().getTime() - startTimeMS) + "】毫妙。");
            logger.info("数据碎片输出情况：" + writeLoad + "】·······················总数据碎片【" + rwTable.getFragmentationNum() + "】\t丢失X【----异步无记录----】\t成功#【----异步无记录----】");
        }
    }

    /**
     * 内部调用，数据碎片输出
     *
     * @param FragmentationOutputStream 数据碎片输出组件Writer组件，用于将数据碎片些出去
     * @param FragmentationN            数据碎片编号，由框架内部自动生成
     * @param rwTable                   数据容器表，这里用于存放数据
     * @throws IOException 数据碎片输出异常
     */
    private void writerFragmentation(Writer FragmentationOutputStream, int FragmentationN, RWTable<String> rwTable) throws IOException {
        writerDTFragmentation(FragmentationOutputStream, rwTable, FragmentationN);
        FragmentationOutputStream.flush();
        FragmentationOutputStream.close();
    }

    /**
     * 将RWTable中的碎片提取出来，并通过"FragmentationOutStream"将数据写出去。
     *
     * @param FragmentationOutStream 输出数据对象 用于指定数据输出路径  Output data object is used to specify the data output path
     * @param rwTable                数据表对象  data table object
     * @param FragmentationNum       数据碎片编号  data fragment number
     * @throws IOException 构造DT数据碎片失败
     */
    protected void writerDTFragmentation(Writer FragmentationOutStream, RWTable<String> rwTable, int FragmentationNum) throws IOException {
        // 通过数据碎片编号，提取指定数据碎片的类
        DataFragmentation orDefault = rwTable.getFragmentationMap().getOrDefault(FragmentationNum, new DataFragmentation(-1));
        orDefault.setOutSplitChar(outSplit);
        // 通过输出流，将类中提取到的数据输出
        FragmentationOutStream.write(
                (orDefault.getFragmentationNum() == -1 ? "NULL：No Data Found Fragmentation[" + FragmentationNum + "];" : orDefault.getData())
                        .getBytes(this.charset == null ? "utf-8" : this.charset));
    }

    /**
     * 关闭通过内置或者自定义获取到的NameManager的数据输入流，对于数据的输出流，在opData中自动的被关闭。
     * <p>
     * Close the data input stream of the Name Manager obtained through built-in or custom, for the data output stream, it is automatically closed in op Data.
     *
     * @return 关闭组件的成功或失败。  The success or failure of shutdown components.
     */
    @Override
    public boolean closeStream() {
        logger.info("输入任务已结束，准备关闭数据输入流。");
        if (reader != null) {
            logger.info("将自定义的Read组件，关闭！");
            try {
                reader.closeStream();
            } catch (IOException e) {
                logger.warn("自定义的Read组件关闭失败，原因：" + e);
                e.printStackTrace();
            }
        }
        if (inputStream != null) {
            try {
                inputStream.close();
                logger.info("将内置的数据流组件，关闭！");
            } catch (IOException e) {
                logger.warn(e.getMessage());
                e.printStackTrace(System.err);
                return false;
            }
        }
        return true;
    }
}
