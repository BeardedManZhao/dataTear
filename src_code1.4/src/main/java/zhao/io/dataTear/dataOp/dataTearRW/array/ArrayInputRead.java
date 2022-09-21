package zhao.io.dataTear.dataOp.dataTearRW.array;

import zhao.io.dataTear.dataOp.dataTearRW.Reader;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.stream.Stream;

/**
 * 直接加载数据到框架中的类，本类自身也是一种数据流，但是允许通过程序内部产生的数据，不用去访问文件系统，适合作为Master中的setReader方法实参
 * 一般会可以用来向框架输入程序中产生的数据
 * <p>
 * 被Deprecated注解的方法，在本类中没有任何的作用，您应该尽量避免这类方法，当然，如果使用了也不会影响您的操作
 * <p>
 * 使用方式:
 * <p>
 * ArrayRead arrayRead = ArrayRead.create(); // 构建数据组件
 * <p>
 * arrayRead.openStream(); // 打开数据流
 * <p>
 * arrayRead.addData(...).addData(...).addData(...); // 允许链式调用添加数据到组件中
 * <p>
 * arrayRead.op_Data(); // 将添加的所有数据一起构建成为一个数据集
 * <p>
 * arrayRead.closeStream(); // 关闭组件
 * <p>
 * arrayRead.getDataArray(); // 提取构建的数据集
 *
 * @see zhao.io.dataTear.dataOp.dataTearRW.DTMaster
 * Master中的函数使用：setReader(arrayRead.create().addData(...))
 */
public class ArrayInputRead extends Reader {
    private static int usedCount = 0;
    private final Date startTime = new Date();
    private final String errorStr = "数据添加至ArrayRead错误，原因：";
    private final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    public ArrayInputRead() {
        usedCount += 1;
    }

    /**
     * 获取本类的方法
     *
     * @return 本类的对象，需要通过建造者模式进行构造
     */
    public static ArrayInputRead create() {
        ArrayInputRead arrayInputRead = new ArrayInputRead();
        arrayInputRead.setIn_File(new File("ArrayRead://" + arrayInputRead.byteArrayOutputStream));
        return arrayInputRead;
    }

    /**
     * 添加数据到组件，这里添加的数据是字符串类型
     *
     * @param data 需要被添加的数据
     * @return 链
     * @throws IOException 数据添加异常
     */
    public ArrayInputRead addData(String data) throws IOException {
        byteArrayOutputStream.write(data.getBytes());
        return this;
    }

    /**
     * 添加数据到组件，这里添加的数据是字符串的集合类型
     *
     * @param data 需要被添加的数据
     * @return 链
     */
    public ArrayInputRead addData(Collection<String> data) {
        return addData(data.stream());
    }

    /**
     * 添加数据到组件，这里添加的数据是字符串类型
     *
     * @param data 需要被添加的数据
     * @return 链
     */
    public ArrayInputRead addData(String... data) {
        return addData(Arrays.stream(data));
    }

    /**
     * 添加数据到组件，这里添加的数据是字符串类型
     *
     * @param data 需要被添加的数据
     * @return 链
     */
    public ArrayInputRead addData(byte[] data) {
        try {
            byteArrayOutputStream.write(data);
        } catch (IOException e) {
            e.printStackTrace(System.err);
            logger.error(errorStr + e);
        }
        return this;
    }

    /**
     * 添加数据到组件，这里添加的数据是字符串数据流类型
     *
     * @param data 需要被添加的数据流
     * @return 链
     */
    public ArrayInputRead addData(Stream<String> data) {
        data.forEach((data_str) -> {
            try {
                byteArrayOutputStream.write(data_str.getBytes());
            } catch (IOException e) {
                e.printStackTrace(System.err);
                logger.error(errorStr + e);
            }
        });
        return this;
    }

    /**
     * @return 该DT文件来自于哪里
     */
    @Override
    public String getSrcFile() {
        try {
            return "ArrayRead://" + InetAddress.getLocalHost().getHostAddress() + "/usedCount = [" + usedCount + "]";
        } catch (UnknownHostException e) {
            return "ArrayRead://XXX.XXX.XXX.XXX/usedCount = " + "[" + usedCount + "]";
        }
    }

    /**
     * @param srcFile 为子类提供的源文件路径设置方法
     * @deprecated 本类不需要使用此方法
     */
    @Override
    protected void setSrcFile(String srcFile) {
    }

    /**
     * 获取文件创建时间毫秒值 注意 如果输入文件非DataTear类型的文件，那么将无法获取文件创建时间
     *
     * @return 文件创建时间的毫秒值 无法获取时返回 -1
     */
    @Override
    public long getCreateDateMS() {
        return startTime.getTime();
    }

    /**
     * 为子类提供的设置数据组件，子类可以通过该组件进行输出数据的设置
     *
     * @param datas Byte数组
     */
    @Override
    protected void setByteArray(byte[] datas) {
        super.setByteArray(datas);
    }

    /**
     * 获取被读取文件的路径，在本类中是绝对路径
     *
     * @return 被读取文件按的路径
     */
    @Override
    public String getIn_FilePath() {
        return getSrcFile();
    }

    /**
     * 通过文件对象的形式，设置需要读取的文件，setIn_File 与 setIn_Path 与 setInputStream 一般来说要设置其中的一个，具体是否需要设置，还需要看实现的子类们是如何获取
     *
     * @param in_FilePath 通过文件路径进行数据输入流的设置
     * @return 链
     */
    @Override
    @Deprecated
    public Reader setIn_FilePath(String in_FilePath) {
        return this;
    }

    /**
     * Reader 是DataTear框架中比较重要的数据加载类，其中可以包含一个全局的数据输入流，自身也可以作为数据输入组件，具体的在于实现
     *
     * @return NULL
     * @deprecated 本类不需要使用此方法
     */
    @Override
    @Deprecated
    public InputStream getInputStream() {
        return null;
    }

    /**
     * 通过文件对象的形式，设置需要读取的文件，setIn_File 与 setIn_Path 与 setInputStream 一般来说要设置其中的一个，具体是否需要设置，还需要看实现的子类们是如何获取
     *
     * @param inputStream 数据输入流设置
     * @return 链
     * @deprecated 本类不需要使用此方法
     */
    @Override
    @Deprecated
    public Reader setInputStream(InputStream inputStream) {
        return this;
    }

    /**
     * 为调用这提供的数据获取方法，该方法返回的数组，可以在被opData运行之后获取出来，请注意，如果opData没有被启动，返回的数据为NULL
     *
     * @return 读取到的byte数据包
     */
    @Override
    public byte[] getDataArray() {
        return super.getDataArray();
    }

    /**
     * 为调用这提供的数据获取方法，该方法返回的数组被自动转化为了字符串，可以在被opData运行之后获取出来，请注意，如果opData没有被启动，返回的数据为NULL
     *
     * @return 读取到的数据的字符串格式返回
     */
    @Override
    public String getDataString() {
        return super.getDataString();
    }

    /**
     * 获取输入文件的对象
     *
     * @return 输入文件对象
     * @deprecated 本类不需要使用此方法
     */
    @Override
    @Deprecated
    public File getIn_File() {
        return super.getIn_File();
    }

    /**
     * 通过文件对象的形式，设置需要读取的文件，setIn_File 与 setIn_Path 与 setInputStream 一般来说要设置其中的一个，具体是否需要设置，还需要看实现的子类们是如何获取
     *
     * @param in_File 输入文件对象
     * @return 链式
     * @deprecated 本类不需要使用此方法
     */
    @Override
    @Deprecated
    public Reader setIn_File(File in_File) {
        return super.setIn_File(in_File);
    }

    @Override
    public boolean openStream() {
        return true;
    }

    @Override
    public boolean op_Data() {
        setByteArray(byteArrayOutputStream.toByteArray());
        return true;
    }

    @Override
    public boolean closeStream() throws IOException {
        byteArrayOutputStream.flush();
        byteArrayOutputStream.close();
        return true;
    }

    /**
     * @return 拆包装方法，本类无需拆包装
     */
    @Override
    @Deprecated
    public Reader toTobject() {
        return this;
    }

    /**
     * opData的调用的另一种形势，不建议使用，可以直接使用opData
     *
     * @return -1
     * @deprecated 本类不需要使用此方法
     */
    @Override
    @Deprecated
    public int read() {
        return -1;
    }

    /**
     * @return 流中的数据大概有多少
     */
    @Override
    public int available() {
        return this.byteArrayOutputStream.size();
    }
}
