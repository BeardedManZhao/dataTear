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
 * @version 1.0
 * 写DataTear数据实现类，读取数据，按照DataTear格式规范转换构建输出文件
 * <p>
 * 采用以自身为建造者设计模式进行实例化，省去建造者类的冗余
 * <p>
 * 是数据撕裂的主要模块，通过该模块可以构建出对应的DataTear目录
 * <p>
 * API调用示例：
 * <p>
 * RW rw = new DTMaster(OutPath -> RW.getDT_UDF_Stream(DT_builtIn_UDF.LOCAL_TEXT).writeStream(s)) // 实现的UDF，这里为了看起来简洁，使用了lambda的样式展示
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
    private String In_FilePath;
    private File In_File;
    private String OUT_FilePath;
    private File OUT_file;
    private BufferedInputStream inputStream;
    private DataSourceFormat dataSourceFormat = DataSourceFormat.built_in;
    private DataOutputFormat dataOutputFormat = DataOutputFormat.built_in;
    private Reader reader;
    private String splitrex = "\\s+";
    private String outSplit = ",";
    private int primaryNum = 0;
    private int FragmentationNum = 3;
    private String charset;
    private boolean useSynchronization = true;

    /**
     * 构建DataTear写数据组件 同时也是DataTearMaster
     *
     * @param udf W_UDF接口实现类 其中代表的是写数据组件的实现, 您可以使用Lambda的方式将数据组件从此接口中返回，本类将会去提取组件
     */
    public DTMaster(W_UDF udf) {
        this.udf = udf;
    }

    /**
     * @return 是否使用同步的方式写数据，默认是true
     */
    public boolean isUseSynchronization() {
        return useSynchronization;
    }

    /**
     * 设置写数据的异同模式 默认是true
     *
     * @param useSynchronization 是否使用同步写数据的布尔值 true 代表使用同步
     * @return 链
     */
    public DTMaster setUseSynchronization(boolean useSynchronization) {
        this.useSynchronization = useSynchronization;
        return this;
    }

    /**
     * 获取输出数据的字符集对象
     *
     * @return 输出DT数据使用的字符集
     */
    public String getCharset() {
        return charset;
    }

    /**
     * 该组件需要使用到的编码集 不一定会生效，但是当需要使用到编码集的时候，会按照您指定的编码集类进行输出字符转码！
     * 为了在下一次读取的时候不会出错误，您可以选择性的使用本方法
     * <p>
     * 注意：如果您不指定，需要使用字符集的时候将默认使用utf-8
     *
     * @param charset 您输出的数据是什么编码集
     * @return 链
     */
    public DTMaster setCharset(String charset) {
        this.charset = charset;
        return this;
    }


    /**
     * 自定义输出组件方法，强制实现，其中会自动使用该方法对接Writer接口，并将该方法运用，如果您使用的是自定义输出模式的话，如果您没有使用自定义的输出模式，那么该方法将不会被调用
     *
     * @param OUT_FilePath 转换结果输出路径
     * @return 通过实现的方案获取到的输出流对象
     * @throws IOException 自定义的流打开失败
     */
    protected OutputStream UDTOutputStream(String OUT_FilePath) throws IOException {
        return this.udf.run(OUT_FilePath);
    }

    /**
     * 通过自定义数据输入组件的方式进行数据的读取，如果使用了这个方法，那么之前的输入设置将会被忽略，框架会通过Reader提取数据，然后在输出
     * <p>
     * 平台中有内置的数据Reader组件，如果您有自定义数据输入的需求，您可以选择重写Reader类
     *
     * @param reader 需要使用的读数据组件
     * @return 链
     * @see Reader
     */
    @Priority("1")
    public DTMaster setReader(Reader reader) {
        this.reader = reader;
        this.In_File = reader.getIn_File();
        this.In_FilePath = reader.getIn_FilePath();
        return this;
    }

    /**
     * 设置数据输出的后的数据分隔符号，当数据输出之后，其结构也是一张表的，在最终DT的构建中是有列分隔符的
     *
     * @param outSplit 输出的数据表中的符号 注意不是正则哦
     * @return 链
     */
    public DTMaster setOutSplit(String outSplit) {
        this.outSplit = outSplit;
        return this;
    }

    /**
     * 设置需要输出的DT数据碎片数量，底层会通过标记数值，对该值取余，将数据输出到对应的文件数据碎片中
     *
     * @param fragmentationNum 您需要将文件拆分成多少数据碎片
     * @return 链式
     */
    public DTMaster setFragmentationNum(int fragmentationNum) {
        this.FragmentationNum = fragmentationNum;
        return this;
    }

    /**
     * 设置需要用来构建索引的字段在列中的序号，从0开始排序，可以理解这个就是一个主键，它会作为索引和数据碎片进行关联
     *
     * @param primaryNum 设置主键在字段中的索引位置 从0开始生效 提取到的对应位置的主键数据将会作为非常重要的桥梁
     * @return 链式
     */
    public DTMaster setPrimaryNum(int primaryNum) {
        this.primaryNum = primaryNum;
        return this;
    }

    /**
     * 设置数据输入的列分割正则，会按照该正则将一份数据的字段分割出来。
     *
     * @param splitrex 数据切分符
     * @return 链
     */
    public DTMaster setSplitrex(String splitrex) {
        this.splitrex = splitrex;
        return this;
    }

    /**
     * 通过自定义的数据输入组件，获取数据，注意 如果该选项为UDF内置，那么必须要使用setReader方法设置数据输入组件，灵活性更强
     *
     * @param dataSourceFormat 是否使用自定义的数据输入组件 默认为使用自定义组件
     * @return 链
     * @see Reader
     */
    public DTMaster ReadFormat(DataSourceFormat dataSourceFormat) {
        this.dataSourceFormat = dataSourceFormat;
        return this;
    }

    /**
     * 通过自定义的数据输出组件，获取数据
     *
     * @param dataSinkFormat 是否使用自定义的数据输入组件 默认为是
     * @return 链
     */
    public DTMaster WriterFormat(DataOutputFormat dataSinkFormat) {
        this.dataOutputFormat = dataSinkFormat;
        return this;
    }

    public String getIn_FilePath() {
        return In_FilePath;
    }

    /**
     * 通过文件对象的形式，设置需要读取的文件，setIn_File 与 setIn_Path 与 setInputStream 一般来说要设置其中的一个，具体是否需要设置，还需要看实现的子类们是如何获取
     *
     * @param in_FilePath 被读取的文件路径
     * @return 链
     */
    public DTMaster setIn_FilePath(String in_FilePath) {
        try {
            In_FilePath = in_FilePath;
            In_File = new File(in_FilePath);
        } catch (NullPointerException n) {
            throw new ZHAOLackOfInformation("您为DTMaster设置的信息不全哦！请将输入路径设置一下！");
        }
        return this;
    }


    public String getOUT_FilePath() {
        return OUT_FilePath;
    }

    /**
     * 该方法是对于DT输出路径的描述，需要指定的是一个目录，当启动opData方法的时候，框架将会自动的将该目录下面的文件全部刷新，然后构建DT目录。
     *
     * @param OUT_FilePath DT路径构建目录
     * @return 链
     */
    public DTMaster setOUT_FilePath(String OUT_FilePath) {
        try {
            this.OUT_FilePath = OUT_FilePath;
            OUT_file = new File(OUT_FilePath);
        } catch (NullPointerException n) {
            throw new ZHAOLackOfInformation("您为DTMaster设置的信息不全哦！请将输出路径设置一下！");
        }
        return this;
    }

    public File getOUT_file() {
        return OUT_file;
    }

    /**
     * 该方法是对DT输出路径的描述，需要指定的是一个目录的File类对象，当启动opData的死后，将会从File中提取输出路径。
     *
     * @param OUT_file 输出路径文件对象
     * @return 链
     */
    public DTMaster setOUT_file(File OUT_file) {
        try {
            this.OUT_file = OUT_file;
            OUT_FilePath = OUT_file.getAbsolutePath();
        } catch (NullPointerException n) {
            throw new ZHAOLackOfInformation("您为DTMaster设置的信息不全哦！请将输出路径对象设置一下！");
        }
        return this;
    }

    /**
     * 内置的加载数据碎片的方法,该方法会被用来转换为RWTable需要的格式，最终会被添加的RWTable中
     *
     * @param readData 需要被转换成表数组的数据String
     * @return 数据表的二维数组
     */
    private ArrayList<String[]> loadData(String readData) {
        ArrayList<String[]> lines = new ArrayList<>();
        for (String line : readData.split("\n")) {
            lines.add(line.split(splitrex));
        }
        return lines;
    }

    /**
     * 打开数据流，同时返回状态，这个数据流的获取会先判断是否是自定义的数据输入模式，如果是的话，将会打开reader的数据流
     *
     * @return 是否成功打开数据流
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
            if (dataSourceFormat == DataSourceFormat.UDT) {
                ZHAOLackOfInformation zhaoLackOfInformation = new ZHAOLackOfInformation("您设置的输入模式/输出模式，与真正使用的组件是不一致的，如果您想要使用" + dataOutputFormat + "模式，请您结束对setInPath一类的方法调用。");
                logger.error("DTReader错误" + zhaoLackOfInformation);
                throw zhaoLackOfInformation;
            } else if (dataSourceFormat == DataSourceFormat.built_in) {
                ZHAOLackOfInformation zhaoLackOfInformation = new ZHAOLackOfInformation("您设置的输入模式/输出模式，与真正使用的组件是不一致的，如果您想要使用" + dataOutputFormat + "模式，请您结束对Reader的设置。");
                logger.error("DTReader错误" + zhaoLackOfInformation);
                throw zhaoLackOfInformation;
            }
            return false;
        }
    }

    /**
     * 操作数据的方法，这里是写数据，如果您是使用的Reader，那么这里将会提取Reader的结果数据
     *
     * @return 是否成功完成写数据操作
     */
    @Override
    public boolean op_Data() {
        byte[] datas = new byte[(int) In_File.length()];
        RWTable<String> rwTable = new RWTable<>();
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
                rwTable.putAllData(loadData(new String(datas)));
                logger.info("数据加载完成！开始进行DataTear的格式转换，构建rwTable");
            } catch (ArrayIndexOutOfBoundsException e) {
                logger.error("您设置的primaryKey索引不存在于数据中，您的数据列数可能小于您设置的primaryIndex序号，请您检查数据表结构。" + e.getLocalizedMessage());
                e.printStackTrace(System.err);
                return false;
            }
            try {
                writerNameManagerandData(rwTable);
            } catch (ArrayIndexOutOfBoundsException e) {
                logger.error("您设置的数据输出流在启动的时候，出现错误了哦！。" + e);
                e.printStackTrace(System.err);
                return false;
            }
            return true;
        } catch (IOException e) {
            if (dataSourceFormat == DataSourceFormat.built_in) {
                logger.error("数据输出流异常：原因：" + e);
            } else {
                logger.error("UDF输出流异常：原因：" + e);
            }
            e.printStackTrace(System.err);
            return false;
        }
    }

    /**
     * 将RWTable数据以DataTear的格式持久化到磁盘，同时将与之关联的数据碎片路径补全
     *
     * @param rwTable 关联的数据碎片对象
     * @throws IOException 构造NameManager失败
     */
    protected void writerNameManagerandData(RWTable<String> rwTable) throws IOException {
        long startTimeMS = new Date().getTime();
        String NameManager_Path = OUT_FilePath + "/NameManager.NDT";
        StringBuilder FragmentationsPath = new StringBuilder();
        boolean isNotUdf = dataOutputFormat == DataOutputFormat.built_in;
        DTWrite NameManagerOutStream = isNotUdf ? DTWrite.bulider().setPath(NameManager_Path).create() : new DTWrite(UDTOutputStream(NameManager_Path), NameManager_Path);
        CountDownLatch countDownLatch = new CountDownLatch(this.FragmentationNum);
        // 通过Fragmentation编号绘制路径，同时向输出对应路径的数据碎片数据，最后标记压缩格式
        final StringBuilder writeLoad = new StringBuilder("【");
        AtomicInteger real = new AtomicInteger();
        AtomicInteger oknum = new AtomicInteger();
        for (int FragmentationN = 0; FragmentationN < rwTable.getFragmentationNum(); FragmentationN++) {
            Writer FragmentationOutputStream;
            final int finalFragmentationN = FragmentationN;
            try {
                String FragmentationPath = OUT_FilePath + "/Fragmentation-" + finalFragmentationN + ".DT";
                FragmentationOutputStream = isNotUdf ? DTWrite.bulider().setPath(FragmentationPath).create() : new DTWrite(UDTOutputStream(FragmentationPath), FragmentationPath);
                FragmentationsPath.append(FragmentationOutputStream.getPath()).append("&");
                new Thread(() -> {
                    try {
                        writerFragmentation(FragmentationOutputStream, finalFragmentationN, rwTable);
                        countDownLatch.countDown();
                        oknum.addAndGet(1);
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
        String nameManagerData = (rwTable.superString() + FragmentationsPath + "\r\n" + "srcFile = " + In_FilePath + "\n");
        NameManagerOutStream.write(nameManagerData.getBytes(this.charset == null ? "utf-8" : this.charset));
        NameManagerOutStream.flush();
        NameManagerOutStream.close();
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
            logger.info("数据碎片输出情况：" + writeLoad + "】·······················总数据碎片【" + rwTable.getFragmentationNum() + "】\t丢失X【" + real.get() + "】\t成功#【" + oknum.get() + "】");
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
     * 将指定数据表的指定数据碎片的数据输出，这里用来将rwTable载入大屏输出流并输出，流需要在该方法的调用者中进行开启或关闭
     *
     * @param FragmentationOutStream 输出数据对象 用于指定数据输出路径
     * @param rwTable                数据表对象
     * @param FragmentationNum       数据碎片编号
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
     * 关闭通过内置或者自定义获取到的NameManager的数据输入流，对于数据的输出流，在opData中自动的被关闭
     *
     * @return 是否关闭成功
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
