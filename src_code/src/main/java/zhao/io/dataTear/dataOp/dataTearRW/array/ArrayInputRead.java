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
 * <h3>中文</h3>
 * 直接加载数据到框架中的类，本类自身也是一种数据流，但是允许通过程序内部产生的数据，不用去访问文件系统，适合作为Master中的setReader方法实参
 * 一般会可以用来向框架输入程序中产生的数据
 * <p>
 * 被Deprecated注解的方法，在本类中没有任何的作用，您应该尽量避免这类方法，当然，如果使用了也不会影响您的操作
 * <h3>English</h3>
 * A class that directly loads data into the framework. This class itself is also a data stream, but it allows data generated inside the program without accessing the file system. It is suitable as an argument of the setReader method in the Master and can generally be used to input to the framework. data generated by the program
 * <p>
 * The methods annotated by Deprecated have no effect in this class. You should try to avoid such methods. Of course, if you use them, it will not affect your operation.
 * <p>
 * 使用方式:
 * <p>
 * ArrayRead arrayRead = ArrayRead.create(); // 构建数据组件
 * <p>
 * arrayRead.openStream(); // 打开数据流
 * <p>
 * arrayRead.addData(...).addData(...).addData(...); // 允许own object, which can be used for chain programming式调用添加数据到组件中
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
     * 获取本数据输入组件，在这个组件中，会生成该数据碎片唯一的URL
     * <p>
     * Get this data input component, in this component, the unique URL of the data fragment will be generated
     *
     * @return 本类的对象，需要通过建造者模式进行构造
     */
    public static ArrayInputRead create() {
        ArrayInputRead arrayInputRead = new ArrayInputRead();
        arrayInputRead.setIn_File(new File("ArrayRead://" + arrayInputRead.byteArrayOutputStream));
        return arrayInputRead;
    }

    /**
     * 添加数据到组件，这里添加的数据是字符串类型，这个字符串数据就是一个待处理的元素，您可以将它理解为单元格
     * <p>
     * Add data to the component, the data added here is a string type, this string data is an element to be processed, you can understand it as a cell.
     *
     * @param data 需要被添加的数据  data to be added
     * @return own object, which can be used for chain programming
     * @throws IOException 数据添加异常
     */
    public ArrayInputRead addData(String data) throws IOException {
        byteArrayOutputStream.write(data.getBytes());
        return this;
    }

    /**
     * 添加数据到组件，这里添加的数据是字符串的集合类型，集合中的每一个元素都可以被理解为是一个单元格
     * <p>
     * Add data to the component, the data added here is a collection type of strings, and each element in the collection can be understood as a cell.
     *
     * @param data 需要被添加的数据  data to be added
     * @return own object, which can be used for chain programming
     */
    public ArrayInputRead addData(Collection<String> data) {
        return addData(data.stream());
    }

    /**
     * 添加数据到组件，这里添加的数据是字符串类型，可添加多个字符串！每一个字符串都可以被理解为是一个单元格
     * <p>
     * Add data to the component, the data added here is a string type, and multiple strings can be added! Each string can be understood as a cell.
     *
     * @param data 需要被添加的数据  data to be added
     * @return own object, which can be used for chain programming
     */
    public ArrayInputRead addData(String... data) {
        return addData(Arrays.stream(data));
    }

    /**
     * 添加数据到组件，这里添加的数据是一个byte数组
     * <p>
     * Add data to the component, the data added here is a byte array.
     *
     * @param data 需要被添加的数据  data to be added
     * @return own object, which can be used for chain programming
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
     * 添加数据到组件，这里添加的数据是字符串数据流类型，流中的每一个元素都可以被理解为是一个单元格
     * <p>
     * Add data to the component, the data added here is a string data stream type, and each element in the stream can be understood as a cell.
     *
     * @param data 需要被添加的数据流  data stream to be added
     * @return own object, which can be used for chain programming
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
     * @return 该DT文件的来源，一般就是成为DT文件之前的文件名称！
     * <p>
     * The source of the DT file is generally the file name before it becomes the DT file!
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
     * @param srcFile 为子类提供的源文件路径设置方法，只能由自己或子类去调用设置，这是一个比较重要的方法，在该数据组件中，这个方法可以直接忽略，如有特殊需求，您可以继续实现拓展。
     *                <p>
     *                The source file path setting method provided for subclasses can only be called and set by itself or subclasses. This is a relatively important method. In this data component, this method can be ignored directly. If you have special needs, you can continue achieve expansion.
     * @deprecated 本类不需要使用此方法，因为数组的源本就是来源于一个程序内部的数组，而不是来自文件系统。
     * <p>
     * This class does not need to use this method, because the source of the array is an array inside a program, not from the file system.
     */
    @Override
    protected void setSrcFile(String srcFile) {
    }

    /**
     * 获取文件创建时间毫秒值 注意 如果输入文件非DataTear类型的文件，那么将无法获取文件创建时间。
     * <p>
     * Get the file creation time in milliseconds Note If the input file is not a DataTear type file, the file creation time cannot be obtained.
     *
     * @return 文件创建时间的毫秒值 无法获取时返回 -1
     * <p>
     * File creation time in milliseconds Returns -1 if not available
     */
    @Override
    public long getCreateDateMS() {
        return startTime.getTime();
    }

    /**
     * 为子类提供的设置数据组件，子类可以通过该组件进行输出数据的设置。
     * <p>
     * A setting data component provided for subclasses, through which subclasses can set output data.
     *
     * @param datas Byte数组
     */
    @Override
    protected void setByteArray(byte[] datas) {
        super.setByteArray(datas);
    }

    /**
     * 获取被读取文件的路径，在本类中是数组碎片的URL。
     * <p>
     * Get the path of the read file, in this class it is the URL of the array fragment.
     *
     * @return 被读取文件按的路径 The path to read the file by.
     */
    @Override
    public String getIn_FilePath() {
        return getSrcFile();
    }

    /**
     * 通过文件对象的形式，设置需要读取的文件，setIn_File 与 setIn_Path 与 setInputStream 一般来说要设置其中的一个，具体是否需要设置，还需要看实现的子类们是如何获取
     * <p>
     * Set the file to be read in the form of a file object. Generally, setIn_File, setIn_Path and setInputStream need to set one of them. Whether it needs to be set depends on how the subclasses of the implementation obtain it.
     *
     * @param in_FilePath 通过文件路径进行数据输入流的设置  Set data input stream through file path
     * @return own object, which can be used for chain programming
     */
    @Override
    @Deprecated
    public Reader setIn_FilePath(String in_FilePath) {
        return this;
    }

    /**
     * Reader 是DataTear框架中比较重要的数据加载类，其中可以包含一个全局的数据输入流，自身也可以作为数据输入组件，具体的在于实现
     * <p>
     * Reader is an important data loading class in the DataTear framework, which can contain a global data input stream, and can also be used as a data input component itself.
     *
     * @return NULL
     * @deprecated This class does not need to use this method, because the source of the array is an array inside a program, not from the file system.
     */
    @Override
    @Deprecated
    public InputStream getInputStream() {
        return null;
    }

    /**
     * 通过文件对象的形式，设置需要读取的文件，setIn_File 与 setIn_Path 与 setInputStream 一般来说要设置其中的一个，具体是否需要设置，还需要看实现的子类们是如何获取
     * <p>
     * Set the file to be read in the form of a file object. Generally, setIn_File, setIn_Path and setInputStream need to set one of them. Whether it needs to be set depends on how the subclasses of the implementation obtain it.
     *
     * @param inputStream 数据输入流设置
     * @return own object, which can be used for chain programming
     * @deprecated This class does not need to use this method, because the source of the array is an array inside a program, not from the file system.
     */
    @Override
    @Deprecated
    public Reader setInputStream(InputStream inputStream) {
        return this;
    }

    /**
     * 为调用这提供的数据获取方法，该方法返回的数组，可以在被opData运行之后获取出来，请注意，如果opData没有被启动，返回的数据为NULL
     * <p>
     * In order to call the data acquisition method provided by this method, the array returned by this method can be obtained after being run by op Data. Please note that if op Data is not started, the returned data is NULL.
     *
     * @return 读取到的byte数据包
     */
    @Override
    public byte[] getDataArray() {
        return super.getDataArray();
    }

    /**
     * 为调用这提供的数据获取方法，该方法返回的数组被自动转化为了字符串，可以在被opData运行之后获取出来，请注意，如果opData没有被启动，返回的数据为NULL
     * <p>
     * To call the data acquisition method provided by this method, the array returned by this method is automatically converted into a string, which can be obtained after being run by op Data. Please note that if op Data is not started, the returned data is NULL.
     *
     * @return 读取到的数据的字符串格式返回
     */
    @Override
    public String getDataString() {
        return super.getDataString();
    }

    /**
     * 获取输入文件的对象
     * <p>
     * Get the object of the input file
     *
     * @return 输入文件对象 input file object
     * @deprecated 本类不需要使用此方法  This class does not need to use this method
     */
    @Override
    @Deprecated
    public File getIn_File() {
        return super.getIn_File();
    }

    /**
     * 通过文件对象的形式，设置需要读取的文件，setIn_File 与 setIn_Path 与 setInputStream 一般来说要设置其中的一个，具体是否需要设置，还需要看实现的子类们是如何获取
     * <p>
     * Set the file to be read in the form of a file object. Generally, setIn_File, setIn_Path and setInputStream need to set one of them. Whether it needs to be set depends on how the subclasses of the implementation obtain it.
     *
     * @param in_File 输入文件对象 input file object
     * @return own object, which can be used for chain programming式
     * @deprecated 本类不需要使用此方法  This class does not need to use this method
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
     * <p>
     * Unpacking method, this class does not need unpacking
     */
    @Override
    @Deprecated
    public Reader toToObject() {
        return this;
    }

    /**
     * opData的调用的另一种形势，不建议使用，可以直接使用opData
     * <p>
     * Another situation of calling op Data, it is not recommended to use op Data directly.
     *
     * @return -1
     * @deprecated 本类不需要使用此方法 This class does not need to use this method
     */
    @Override
    @Deprecated
    public int read() {
        return -1;
    }

    /**
     * @return 流中的数据量的大概估值，用于外界的数据获取
     * <p>
     * An approximate estimate of the amount of data in the stream for external data acquisition
     */
    @Override
    public int available() {
        return this.byteArrayOutputStream.size();
    }
}
