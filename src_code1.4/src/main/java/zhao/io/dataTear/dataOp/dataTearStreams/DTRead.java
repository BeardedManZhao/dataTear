package zhao.io.dataTear.dataOp.dataTearStreams;

import zhao.io.dataTear.atzhaoPublic.R_UDF;
import zhao.io.dataTear.dataOp.dataTearRW.Filter;
import zhao.io.dataTear.dataOp.dataTearRW.Reader;
import zhao.io.ex.AnalysisMetadataException;
import zhao.io.ex.ZHAOLackOfInformation;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author 赵凌宇
 * 您可以通过本类将DataTear数据进行读取
 * DataTear文件存储的读取组件 也是所有的自定义组件都应该实现的类，因为只有这个类包含DataTear输出规范
 */
public class DTRead extends Reader {

    /**
     * 元数据所有索引信息
     */
    private final HashMap<String, String> IndexList = new HashMap<>();
    /**
     * 数据碎片信息
     */
    private final HashMap<String, String> DataFragmentation = new HashMap<>();
    /**
     * 读取数据需要的组件实现
     */
    private final R_UDF udf;
    /**
     * 元数据管理文件输入流，会从UDFInputStream中获取
     */
    private Reader nameManagerInputStream;
    /**
     * 主键描述，符合该描述的主键所在的数据碎片，将会被记录读取
     */
    private Filter PrimaryCharacteristic;
    /**
     * NameManager文件的大小
     * NameManager文件的大小
     */
    private long NameManagerLength;
    /**
     * 累计读取的数据长度
     */
    private long FileLenCount;
    /**
     * 是否使用多线程读取数据
     */
    private boolean useMultithread = true;
    /**
     * 读取NameManager所使用的时间，用于计算数据碎片数据加载等待时间
     */
    private long NameManagerLoadMS;
    /**
     * 读取数据最大超时时间毫米值 在并发数据的情况下，当一个数据读取线程迟迟没有结束的时候，将会使用该数值进行超时限制
     */
    private long MaxOutTimeMS = 0b000100000000000000000000;

    /**
     * 构造一个数据输入组件，注意，如果数据输入模式为内置，那么您的构造参数可以为null，如果不是，请在这里实现接口并将其对接到这里
     *
     * @param udf 数据输入组件的对接接口 您可以使用Lambda的方式将数据组件从此接口中返回，本类将会去提取组件
     */
    public DTRead(R_UDF udf) {
        this.udf = udf;
    }

    /**
     * @return NameManager的数据长度数值
     */
    public long getNameManagerLength() {
        return NameManagerLength;
    }

    /**
     * @return 是否使用的并发读取数据
     */
    public boolean isUseMultithread() {
        return useMultithread;
    }

    /**
     * 设置数据加载模式，是否使用多线程并发加载
     *
     * @param useMultithread true 代表并发加载
     * @return 链
     */
    public DTRead setUseMultithreading(boolean useMultithread) {
        this.useMultithread = useMultithread;
        if (useMultithread) {
            logger.info("将按照您的需求，使用多线程数据加载方式，这种方式在您读取大数据量或复杂UDF实现时将会极其的优秀。");
        } else {
            logger.warn("您并未启动多线程，针对 setUseMultithreading() 方法形参传入是一个false，而这个参数是框架的默认数值，单线程读取数据的方式您不需要设置。当然，这不会影响您的操作执行。");
        }
        return this;
    }

    /**
     * @return 读取数据最大超时时间毫米值
     */
    public long getMaxOutTimeMS() {
        return MaxOutTimeMS;
    }

    /**
     * 设置并发数据读取最大超时时间
     *
     * @param maxOutTimeMS 时间毫秒值
     * @return 链
     */
    public DTRead setMaxOutTimeMS(long maxOutTimeMS) {
        logger.info("设置并发情况下加载数据的最大超时时间(ms)：" + MaxOutTimeMS + " -> " + maxOutTimeMS);
        MaxOutTimeMS = maxOutTimeMS;
        return this;
    }

    /**
     * 自定义数据输入组件插入到本类中, 该方法会返回一个Reader读数据组件，这个组件将会被Read类全程使用
     *
     * @param In_FilePath 数据路径
     * @return 数据输入流
     * @throws IOException 自定义数据输入Reader组件构建异常。
     */
    protected Reader UDTInputStream(String In_FilePath) throws IOException {
        return this.udf.run(In_FilePath);
    }

    /**
     * @param in_FilePath NameManager文件的路径
     * @return 链
     */
    @Override
    public Reader setIn_FilePath(String in_FilePath) {
        return super.setIn_FilePath(in_FilePath);
    }

    /**
     * @param in_File NameManager文件的对象
     * @return 链
     */
    @Override
    public Reader setIn_File(File in_File) {
        return super.setIn_File(in_File);
    }

    /**
     * 提取数据流对象
     *
     * @return 将Reader组件中的流进行单提取
     */
    @Override
    public InputStream getInputStream() {
        return this.nameManagerInputStream.getInputStream();
    }

    /**
     * 通过Lambda，对主键进行描述，用来定位满足条件的主键所在数据碎片
     *
     * @param primaryCharacteristic 通过布尔值描述需要查找的主键的特征
     * @return 链
     */
    public DTRead setPrimaryCharacteristic(Filter primaryCharacteristic) {
        this.PrimaryCharacteristic = primaryCharacteristic;
        return this;
    }

    /**
     * 解析NameManager文件数据信息，其中使用到的元数据管理的输入流对象，因此这个对象需要在该方法调用前被启动
     *
     * @throws IOException 非NameManager，或损坏，或不可读取
     */
    private void AnalysisMetadata() throws IOException {
        if (PrimaryCharacteristic != null) {
            ByteArrayOutputStream MetaBuilder = new ByteArrayOutputStream();
            int Filelen = (int) super.getIn_File().length();
            FileLenCount = Filelen > 7 ? Filelen : nameManagerInputStream.available();
            NameManagerLength = nameManagerInputStream.available();
            nameManagerInputStream.op_Data();
            try {
                MetaBuilder.write(nameManagerInputStream.getDataArray());
            } catch (IOException | NullPointerException e) {
                throw new AnalysisMetadataException("有尝试解析元数据，但是没有解析成功！ 异常原因：" + (nameManagerInputStream.getDataArray() == null ? "元数据的输入流没有被加载" : "元数据解析数组没有被存入数组") + "。");
            }
            String[] mateLines = MetaBuilder.toString().trim().split("\n");
            MetaBuilder.flush();
            MetaBuilder.close();
            logger.info("读取NameManager：[" + super.getIn_FilePath() + "]·······读取大小：" + FileLenCount);
            HashSet<String> NoFragmentations = new HashSet<>(); // 不需要的Fragmentation将会被存储进这里
            System.out.print("开始提取NameManager【");
            // 提取所有键值对信息 同时将符合查询索引条件的数据碎片提取
            for (String mateLine : mateLines) {
//                System.err.println(mateLine);
                String[] split = mateLine.split("\\s+=\\s+");
                String NameMetakey = split[0];
                String primary_key = split.length > 1 ? split[1] : " ";
                String primary_blk = NameMetakey.split("@")[0];
                boolean isblk = primary_blk.startsWith("Fragmentation-");
                boolean isadd = DataFragmentation.containsKey(primary_blk); // 数据碎片是否被添加过
                // 提取元数据 以及 数据碎片信息
                if (!isadd && isblk && PrimaryCharacteristic.filter(primary_key)) {
                    // 如果NameManager中存在该数据碎片元数据 && 该数据碎片的主键进行初次定位 && 该数据碎片的主键进行匹配描述
                    DataFragmentation.put(primary_blk, null);
                    System.out.print("#");
                } else if (!isblk) {
                    // 如果不是数据碎片信息 就是索引 需要添加进索引列表
                    IndexList.put(NameMetakey, primary_key);
                    System.out.print("$");
                } else if (!isadd && !NoFragmentations.contains(primary_blk)) {
                    // 如果是我们不需要的数据碎片
                    System.out.print("=");
                    NoFragmentations.add(primary_blk);
                }
            }
            super.setSrcFile(IndexList.getOrDefault("srcFile", "----丢失----"));
            System.out.println("】\n* >>> 提取到需要的数据碎片(#)【" + DataFragmentation.size() + "】\t未提取数据碎片(=)【" + NoFragmentations.size() + "】\t其它索引信息($)【" + IndexList.size() + "】");
            // 提取我们需要的数据碎片路径
            for (String FragmentationPath : IndexList.get("zhao.NameManager.Fragmentation.path").split("&+")) {
                File FragmentationFile = new File(FragmentationPath);
                String FragmentationName = FragmentationFile.getName();
                if (DataFragmentation.containsKey(FragmentationName)) {
                    DataFragmentation.put(FragmentationName, FragmentationPath);
                }
            }
        } else {
            ZHAOLackOfInformation zhaoLackOfInformation = new ZHAOLackOfInformation("您设置的信息不全哦！请对DataTearRead类的PrimaryCharacteristic进行设置，否则没有办法找到您需要的数据碎片了呢。");
            logger.error(zhaoLackOfInformation.getLocalizedMessage());
            zhaoLackOfInformation.printStackTrace();
            throw zhaoLackOfInformation;
        }
    }

    /**
     * 打开元数据输入流，同时解析元数据，如果成功返回true
     *
     * @return 打开元数据输入流，同时解析元数据，如果成功返回true
     */
    @Override
    public boolean openStream() {
        try {
            nameManagerInputStream = UDTInputStream(getIn_FilePath());
            nameManagerInputStream.openStream();
            AnalysisMetadata();
            CreateDateMS = Long.parseLong(IndexList.getOrDefault("zhao.NameManager.id", "0"));
        } catch (NullPointerException | IOException e) {
            AnalysisMetadataException analysisMetadataException = new AnalysisMetadataException("自定义数据流可能发生错误，如果自定义的流没有问题，那么就是NameManager已损坏。");
            logger.error(analysisMetadataException + " and " + e);
            e.printStackTrace(System.err);
            throw analysisMetadataException;
        }
        return true;
    }

    /**
     * 将数据加载进Fragmentation的第二层调用 对数据碎片的数据加载方法，会将数据加载到StringBuilder中
     *
     * @param FragmentationInputStream 数据碎片的输入流，加载数据碎片数据
     * @param stringBuilder            数据碎片的缓冲字符串，同步流中的数据碎片数据
     * @param FragmentationPath        数据碎片路径，会被log4j打印
     * @throws IOException 数据碎片读取异常
     */
    private void Fragmentation(Reader FragmentationInputStream, StringBuilder stringBuilder, String FragmentationPath) throws IOException {
        final long startFileLen = (int) new File(FragmentationPath).length();
        final long fileLen = startFileLen != 0 ? startFileLen : FragmentationInputStream.available();
        FragmentationInputStream.op_Data();
        FileLenCount += fileLen;
        logger.info("读取DataFragmentation：[" + FragmentationPath + "]·············数据碎片大小：" + fileLen);
        addManager(stringBuilder, FragmentationInputStream);
    }

    /**
     * 将数据加载进Fragmentation的第三层调用，数据已经被加载好了，现在需要将所有数据快的数据集中到容器中
     *
     * @param stringBuilder            数据容器，最终的数据快汇总将会到这里，这个方法会大大加快速度，但是缺点就是他是不安全的，因此加载的时候，这里将不会并行，也仅仅只有这里不会并行
     * @param FragmentationInputStream 数据快加载组件，其中包含提取数据的方法，是Reader接口的实现
     */
    private synchronized void addManager(StringBuilder stringBuilder, Reader FragmentationInputStream) {
        stringBuilder.append(new String(FragmentationInputStream.getDataArray()).trim()).append("\n");
    }

    /**
     * 将数据加载进Fragmentation的第一层调用 构造出来数据碎片的输入流，同时对输入流进行管理
     *
     * @param FragmentationPath Fragmentation路径
     * @param stringBuilder     数据存储器
     * @return 操作情况
     */
    private boolean op_Fragmentation(String FragmentationPath, StringBuilder stringBuilder) {
        if (FragmentationPath != null) {
            Reader FragmentationInputStream = null;
            try {
                FragmentationInputStream = UDTInputStream(FragmentationPath);
                if (FragmentationInputStream.openStream()) {
                    Fragmentation(FragmentationInputStream, stringBuilder, FragmentationPath);
                } else {
                    throw new IOException("数据碎片输入流尝试打开，但是发生了错误，打开失败。");
                }
            } catch (IOException e) {
                e.printStackTrace(System.err);
                logger.warn(this.getClass().getName() + "的数据加载进缓冲区失败！原因：" + e);
                return false;
            } finally {
                try {
                    Objects.requireNonNull(FragmentationInputStream).closeStream();
                } catch (IOException | NullPointerException e) {
                    e.printStackTrace(System.err);
                    logger.warn(this.getClass().getName() + "的数据碎片输入流未能正常关闭。原因：" + e);
                }
            }
        } else {
            logger.warn("有数据碎片文件File对象为Null。原因：数据碎片或其元数据丢失，如果NameManager没有损坏，那么就是数据碎片丢失了！");
        }
        return true;
    }

    /**
     * 解析所有数据碎片数据，同时合并所有块数据
     *
     * @return 所有数据碎片是否全部解析成功
     */
    @Override
    public boolean op_Data() {
        StringBuilder stringBuilder = new StringBuilder();
        ArrayList<Boolean> Allstatus = new ArrayList<>();
        int AllCount = DataFragmentation.size();
        CountDownLatch countDownLatch = new CountDownLatch(AllCount);
        int okCount = 0b1;
        for (String FragmentationPath : DataFragmentation.values()) {
            if (isUseMultithread()) {
                new Thread(() -> {
                    try {
                        Allstatus.add(op_Fragmentation(FragmentationPath, stringBuilder));
                    } catch (NullPointerException n) {
                        logger.warn("数据碎片读取出现错误：" + n);
                        n.printStackTrace(System.err);
                    } finally {
                        countDownLatch.countDown();
                    }
                }, "ReadThread[" + okCount + "]").start();
            } else {
                Allstatus.add(op_Fragmentation(FragmentationPath, stringBuilder));
            }
            if (okCount < AllCount) {
                okCount += 1;
            }
        }
        boolean isOutTime = false;
        if (isUseMultithread()) {
            try {
                // 等待所有数据碎片读取完成/等待到最大超时时间
                isOutTime = !countDownLatch.await(MaxOutTimeMS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        Boolean aBoolean = Allstatus.stream().reduce((x, y) -> x && y).orElse(false);
        if (aBoolean) {
            setByteArray(stringBuilder.toString().getBytes());
            logger.info("* >>> ReadData_ok!!! 数据加载完成··························磁盘累计读取数据量：" + (FileLenCount > 1024 ? "【" + getFileSize(FileLenCount >> 10) + "】" : "【" + FileLenCount + "B】"));
            return true;
        } else if (isOutTime) {
            logger.error("* >>> ReadData_超时!!! 数据加载超时··························数据加载最大超时时间限制（毫秒）：" + MaxOutTimeMS + " MS");
            return false;
        } else {
            logger.info("* >>> ReadData_error!!! 数据加载失败··························磁盘累计读取数据量：" + (FileLenCount > 1024 ? "【" + getFileSize(FileLenCount >> 10) + "】" : "【" + FileLenCount + "B】"));
            return false;
        }
    }

    /**
     * 获取最大到Gb的文件大小转换
     *
     * @param FileLen_K 文件Kb单位数值
     * @return Mb或Gb
     */
    private String getFileSize(Long FileLen_K) {
        if (FileLen_K < 1024) {
            return Math.round(FileLen_K) + " Kb";
        } else if (FileLen_K > 1024) {
            return FileLen_K / 1024.0 + " Mb";
        } else {
            return FileLen_K / 1024.0 + " Gb";
        }
    }

    /**
     * 关闭元数据输入流，数据碎片流在opData中已经被关闭
     *
     * @return 是否关闭成功！
     */
    @Override
    public boolean closeStream() {
        if (nameManagerInputStream != null) {
            try {
                nameManagerInputStream.closeStream();
                logger.info("已关闭数据输入流！");
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("数据输入流关闭失败 原因 ： " + e);
                return false;
            }
        }
        return true;
    }
}
