package zhao.io.dataTear.dataOp.dataTearRW;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zhao.io.dataTear.atzhaoPublic.W_UDF;
import zhao.io.dataTear.dataContainer.DataFragmentation;
import zhao.io.dataTear.dataContainer.RWTable;
import zhao.io.dataTear.dataOp.DataOutputFormat;
import zhao.io.dataTear.dataOp.DataSourceFormat;
import zhao.io.dataTear.dataOp.dataTearStreams.DTWrite;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * @author 赵凌宇
 * @version 1.4.2
 * <p>
 * 写DataTear数据实现类，读取数据，按照DataTear格式规范转换构建输出文件
 * <p>
 * 采用以自身为建造者设计模式进行实例化，省去建造者类的冗余
 * <p>
 * 是数据撕裂的主要模块，通过该模块可以构建出对应的DataTear目录
 */
public final class DTMaster implements RW {
    private final static Logger LOGGER = LoggerFactory.getLogger("DataTear_Core");
    private final W_UDF udf;
    private final int threshold;
    private final String In_FilePath;
    private final File In_File;
    private final String OUT_FilePath;
    private final File OUT_file;
    private final DataSourceFormat dataSourceFormat;
    private final DataOutputFormat dataOutputFormat;
    private final Reader reader;
    private final Pattern splitRex;
    private final String outSplit;
    private final int primaryNum;
    private final int FragmentationNum;
    private final String charset;
    private final boolean useSynchronization;
    private BufferedReader inputStream;

    /**
     * 使用内置的数据输出组件构造一个DTMaster
     * <p>
     * Construct a DT Master using built-in data output components
     *
     * @apiNote 该无参构建DTMaster的方式从1.4.2版本开始支持，在之前的版本您想要使用此功能，需要通过"DTMaster(W_UDF udf)"进行构造，调用了该方法之后，您将不允许设置该组件的"WriterFormat"
     * <p>
     * This method of constructing DTMaster without parameters is supported from version 1.4.2. If you want to use this function in previous versions, you need to construct it through "DTMaster(W_UDF udf)". After calling this method, you will not be allowed to set this function. The component's "WriterFormat"
     */
    public DTMaster(DTMasterBuilder dtMasterBuilder) {
        this.udf = dtMasterBuilder.udf;
        this.threshold = dtMasterBuilder.threshold;
        this.In_FilePath = dtMasterBuilder.In_FilePath;
        this.In_File = dtMasterBuilder.In_File;
        this.OUT_FilePath = dtMasterBuilder.OUT_FilePath;
        this.OUT_file = dtMasterBuilder.OUT_file;
        this.dataSourceFormat = dtMasterBuilder.dataSourceFormat;
        this.dataOutputFormat = dtMasterBuilder.dataOutputFormat;
        this.reader = dtMasterBuilder.reader;
        this.splitRex = Pattern.compile(dtMasterBuilder.splitRex);
        this.outSplit = dtMasterBuilder.outSplit;
        this.primaryNum = dtMasterBuilder.primaryNum;
        this.FragmentationNum = dtMasterBuilder.FragmentationNum;
        this.charset = dtMasterBuilder.charset;
        this.useSynchronization = dtMasterBuilder.useSynchronization;
    }

    /**
     * 开始构建本组件，这里是无参构建，代表使用的是内置组件进行数据构建。
     * <p>
     * Start to build this component. This is parameterless construction, which means that built-in components are used for data construction.
     *
     * @return 构建本组件的builder类
     */
    public static DTMasterBuilder builder() {
        return new DTMasterBuilder();
    }

    /**
     * 开始构建本组件，这里是使用自定义的数据输出组件构建，嗲表的是使用自定义组件进行数据构建。
     * <p>
     * Start to build this component. Here, we will use the user-defined data output component to build.
     *
     * @param udf 自定义的数据输出组件
     * @return 构建本组件的builder类
     */
    public static DTMasterBuilder builder(W_UDF udf) {
        return new DTMasterBuilder(udf);
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
     * 自定义输出组件方法，强制实现，其中会自动使用该方法对接Writer接口，并将该方法运用，如果您使用的是自定义输出模式的话，如果您没有使用自定义的输出模式，那么该方法将不会被调用。
     * <p>
     * Custom output component method, mandatory implementation, which will automatically use this method to connect to the Writer interface, and use this method, if you are using a custom output mode, if you do not use a custom output mode, then this method not be called.
     *
     * @param OUT_FilePath 转换结果输出路径  Conversion result output path
     * @return 通过实现的方案获取到的输出流对象  The output stream object obtained through the implemented scheme
     * @throws IOException 自定义的流打开失败  Custom stream open failed
     */
    private OutputStream UDTOutputStream(String OUT_FilePath) throws IOException {
        return this.udf.run(OUT_FilePath);
    }

    public String getIn_FilePath() {
        return In_FilePath;
    }

    public String getOUT_FilePath() {
        return OUT_FilePath;
    }

    public File getOUT_file() {
        return OUT_file;
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
        return loadData(LINE_SPLIT.split(readData));
    }

    private ArrayList<String[]> loadData(String[] lines) {
        final ArrayList<String[]> linesList = new ArrayList<>(lines.length);
        for (int i = 0, j = lines.length - 1; i < j; ++i, --j) {
            linesList.add(splitRex.split(lines[i]));
            linesList.add(splitRex.split(lines[j]));
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
        LOGGER.info("准备初始化数据流。");
        try {
            if (dataSourceFormat == DataSourceFormat.UDT) {
                LOGGER.info("以自定义组件进行数据输入流的打开。进入到自定义组件[" + reader + "]函数栈。");
                return reader.openStream();
            } else {
                LOGGER.info("以内置组件的方式打开数据流。");
                if (reader != null) {
                    LOGGER.warn("您的输入模式以及数据输入的组件设置不一致！设置的时候使用了自定义的Read组件，但是您又设置数据加载模式为内置，这会导致程序可能不会去启动您的Read类。");
                }
                inputStream = new BufferedReader(new FileReader(In_File));
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace(System.err);
            return false;
        } catch (NullPointerException n) {
            LOGGER.error("发生了空指针异常，具体原因请看堆栈信息。", n);
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
        RWTable<String> rwTable = new RWTable<>(this.threshold);
        rwTable.setPrimaryKeyNum(primaryNum).setFragmentationNum(FragmentationNum);
        try {
            if (dataSourceFormat == DataSourceFormat.built_in) {
                LOGGER.info("使用内置组件的方式开始载入缓冲区数据。");
                if (inputStream.ready()) {
                    String[] split = splitRex.split(inputStream.readLine());
                    if (split.length - 1 >= this.primaryNum) {
                        while (inputStream.ready()) {
                            rwTable.putData(splitRex.split(inputStream.readLine()));
                        }
                    } else {
                        LOGGER.error("您设置的primaryKey索引不存在于数据中，您的数据列数可能小于您设置的primaryIndex序号，请您检查数据表结构,并重新 setPrimaryNum()。");
                        return false;
                    }
                }
            } else {
                reader.op_Data();
                LOGGER.info("使用自定义组件开始载入缓冲区数据。");
            }
            try {
                writerNameManagerAndData(rwTable);
            } catch (ArrayIndexOutOfBoundsException e) {
                LOGGER.error("您设置的数据输出流在启动的时候，出现错误了哦！。" + e);
                e.printStackTrace(System.err);
                return false;
            }
            return true;
        } catch (IOException e) {
            if (dataSourceFormat == DataSourceFormat.built_in) {
                LOGGER.error("数据输出流异常：原因：" + e, e);
            } else {
                LOGGER.error("UDF输出流异常：原因：" + e, e);
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
    private void writerNameManagerAndData(RWTable<String> rwTable) throws IOException {
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
                        LOGGER.error("Fragmentation[" + finalFragmentationN + "] 输出异常，原因：" + e);
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
        LOGGER.info("开始绘制NameManager，并构建索引。");
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
                LOGGER.info("请稍等，正在同步并发写数据中...");
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
        }
        if (isUseSynchronization()) {
            LOGGER.info("NameManager以及Fragmentation 绘制完成····················共耗时【" + (new Date().getTime() - startTimeMS) + "】毫妙。");
            LOGGER.info("数据碎片输出情况：" + writeLoad + "】·······················总数据碎片【" + rwTable.getFragmentationNum() + "】\t丢失X【" + real.get() + "】\t成功#【" + okNum.get() + "】");
        } else {
            LOGGER.warn("请注意，异步方式写数据将依赖于主进程生命周期，请确保主进程不会退出，否则将导致数据输出不全。");
            LOGGER.info("NameManager绘制完成·····························共耗时【" + (new Date().getTime() - startTimeMS) + "】毫妙。");
            LOGGER.info("数据碎片输出情况：" + writeLoad + "】·······················总数据碎片【" + rwTable.getFragmentationNum() + "】\t丢失X【----异步无记录----】\t成功#【----异步无记录----】");
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
    private void writerDTFragmentation(Writer FragmentationOutStream, RWTable<String> rwTable, int FragmentationNum) throws IOException {
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
        LOGGER.info("输入任务已结束，准备关闭数据输入流。");
        if (reader != null) {
            LOGGER.info("将自定义的Read组件，关闭！");
            try {
                reader.closeStream();
            } catch (IOException e) {
                LOGGER.warn("自定义的Read组件关闭失败，原因：" + e);
                e.printStackTrace();
            }
        }
        if (inputStream != null) {
            try {
                inputStream.close();
                LOGGER.info("将内置的数据流组件，关闭！");
            } catch (IOException e) {
                LOGGER.warn(e.getMessage());
                e.printStackTrace(System.err);
                return false;
            }
        }
        return true;
    }
}
