package zhao.io.dataTear.dataOp.dataTearRW;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zhao.io.dataTear.atzhaoPublic.Product;
import zhao.io.ex.ZHAOLackOfInformation;

import java.io.*;


/**
 * 读数据默认实现类, 同时也充当所有读数据组件的接口
 *
 * @author 赵凌宇
 * @version 1.0
 * 需要被重写的核心方法，如果您不毛满足于自定义的组件，您可以通过重写来达到自定义的功能。
 * 该类不需要定制字符集，因为读取数据的时候会按照原先输出DT的字符集进行智能编码集获取。
 * 与DTMaster同属于一个接口，读写API的调用统一
 */
public class Reader extends InputStream implements RW, Product<Reader> {
    protected final static Logger logger = LoggerFactory.getLogger("DataTear_LoadUDFStream");
    protected long CreateDateMS = 0b0;
    private File In_File;
    private String In_FilePath;
    private String srcFile;
    private InputStream inputStream;
    private java.io.Reader inputReaderStream;
    private byte[] dataArray;

    /**
     * @return 该DT文件的来源
     */
    public String getSrcFile() {
        return srcFile;
    }

    /**
     * @param srcFile 为子类提供的源文件路径设置方法
     */
    protected void setSrcFile(String srcFile) {
        this.srcFile = srcFile;
    }

    /**
     * 获取文件创建时间毫秒值 注意 如果输入文件非DataTear类型的文件，那么将无法获取文件创建时间
     *
     * @return 文件创建时间的毫秒值 无法获取时返回 -1
     */
    public long getCreateDateMS() {
        return CreateDateMS == 0 ? -1 : CreateDateMS;
    }

    /**
     * 为子类提供的设置数据组件，子类可以通过该组件进行输出数据的设置
     *
     * @param datas Byte数组
     */
    protected void setByteArray(byte[] datas) {
        this.dataArray = datas;
    }

    /**
     * 获取被读取文件的路径，在本类中是绝对路径
     *
     * @return 被读取文件按的路径
     */
    public String getIn_FilePath() {
        return In_FilePath;
    }

    /**
     * 通过文件对象的形式，设置需要读取的文件，setIn_File 与 setIn_Path 与 setInputStream 一般来说要设置其中的一个，具体是否需要设置，还需要看实现的子类们是如何获取
     *
     * @param in_FilePath 通过文件路径进行数据输入流的设置
     * @return 链
     */
    public Reader setIn_FilePath(String in_FilePath) {
        try {
            this.In_FilePath = in_FilePath;
            In_File = new File(In_FilePath);
            this.srcFile = In_FilePath;
        } catch (NullPointerException n) {
            throw new ZHAOLackOfInformation("您设置的信息不全哦。请将输入路径设置下！");
        }
        return this;
    }

    /**
     * Reader 是DataTear框架中比较重要的数据加载类，其中可以包含一个全局的数据输入流，自身也可以作为数据输入组件，具体的在于实现
     *
     * @return Reader中包含的数据输入流 如果有设置过输入流的话
     */
    public InputStream getInputStream() {
        return inputStream;
    }

    /**
     * 通过文件对象的形式，设置需要读取的文件，setIn_File 与 setIn_Path 与 setInputStream 一般来说要设置其中的一个，具体是否需要设置，还需要看实现的子类们是如何获取
     *
     * @param inputReaderStream 数据输入流设置
     * @return 链
     */
    public Reader setInputStream(InputStream inputReaderStream) {
        this.inputStream = inputReaderStream;
        return this;
    }

    public java.io.Reader getInputReaderStream() {
        return inputReaderStream;
    }

    public Reader setInputReaderStream(java.io.Reader inputReaderStream) {
        this.inputReaderStream = inputReaderStream;
        return this;
    }

    /**
     * 为调用这提供的数据获取方法，该方法返回的数组，可以在被opData运行之后获取出来，请注意，如果opData没有被启动，返回的数据为NULL
     *
     * @return 读取到的byte数据包
     */
    public byte[] getDataArray() {
        return dataArray;
    }

    /**
     * 为调用这提供的数据获取方法，该方法返回的数组被自动转化为了字符串，可以在被opData运行之后获取出来，请注意，如果opData没有被启动，返回的数据为NULL
     *
     * @return 读取到的数据的字符串格式返回
     */
    public String getDataString() {
        return new String(dataArray);
    }

    /**
     * 获取输入文件的对象
     *
     * @return 输入文件对象
     */
    public File getIn_File() {
        return In_File;
    }

    /**
     * 通过文件对象的形式，设置需要读取的文件，setIn_File 与 setIn_Path 与 setInputStream 一般来说要设置其中的一个，具体是否需要设置，还需要看实现的子类们是如何获取
     *
     * @param in_File 输入文件对象
     * @return 链式
     */
    public Reader setIn_File(File in_File) {
        try {
            In_File = in_File;
            In_FilePath = in_File.getAbsolutePath();
            this.srcFile = In_FilePath;
        } catch (NullPointerException n) {
            throw new ZHAOLackOfInformation("您设置的信息不全哦。请将输入文件对象设置下！");
        }
        return this;
    }

    @Override
    public boolean openStream() {
        try {
            if (inputStream == null) {
                inputStream = new DataInputStream(new FileInputStream(In_File));
            }
            logger.info("DataTear数据输入流已经被打开。");
            return true;
        } catch (FileNotFoundException e) {
            logger.error("您设置的输入文件路径有误，DT_API不能根据您的路径找到数据文件。");
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean op_Data() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            IOUtils.copy(inputStream, byteArrayOutputStream);
            byteArrayOutputStream.flush();
            dataArray = byteArrayOutputStream.toByteArray();
            logger.info("DataTear数据输入流加载数据到环形缓冲区。");
            byteArrayOutputStream.close();
            return true;
        } catch (IOException e) {
            logger.error("数据加载没有成功，异常原因：" + e);
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean closeStream() throws IOException {
        try {
            inputStream.close();
            logger.info("DataTear数据输入流被关闭。");
        } catch (IOException e) {
            logger.error("数据输入流关闭没有成功，异常原因：" + e);
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * @return 拆包装方法，本类无需拆包装
     */
    @Override
    @Deprecated
    public Reader toToObject() {
        logger.warn("您使用的 toToObject() 方法在Reader类中毫无作用，不会影响程序，但是并不建议使用。");
        return this;
    }

    /**
     * opData的调用的另一种形势，不建议使用，可以直接使用opData
     *
     * @return -1
     * @throws IOException opData的异常
     */
    @Override
    @Deprecated
    public int read() throws IOException {
        op_Data();
        return -1;
    }

    /**
     * @return 流中的数据大概有多少
     * @throws IOException 流估算异常
     */
    @Override
    public int available() throws IOException {
        return inputStream.available();
    }

    protected String readLine() {return null;}
}
