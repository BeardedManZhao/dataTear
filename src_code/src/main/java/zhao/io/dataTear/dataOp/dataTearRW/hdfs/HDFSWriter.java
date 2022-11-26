package zhao.io.dataTear.dataOp.dataTearRW.hdfs;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import zhao.io.dataTear.dataOp.dataTearRW.Writer;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * 向HDFS分布式集群中写数据的组件
 * 该组件将会被DTMaster调用，按照DT的格式输出数据
 * <p>
 * The component that writes data to the HDFS distributed cluster This component will be called by DTMaster and output data in DT format
 */
public class HDFSWriter extends Writer {

    protected final FSDataOutputStream fsDataOutputStream;
    Path Out_path;
    String Out_Pathstr;
    private Charset charset;

    /**
     * 由建造者进行构造函数的执行，获取到这个类
     * <p>
     * The constructor executes the constructor to obtain this class
     *
     * @param fileSystem HDFS对象，会通过该对象获取HDFS集群地址
     *                   <p>
     *                   HDFS object, through which the HDFS cluster address will be obtained
     * @param path       数据输出路径  data output path
     * @param charset    数据输出编码集  Data output code set
     * @throws IOException 无法通过HDFS对象获取目标HDFS 或 Path错误的时候，都有可能抛出该错误
     *                     <p>
     *                     This error may be thrown when the target HDFS or Path error cannot be obtained through the HDFS object
     */
    public HDFSWriter(FileSystem fileSystem, Path path, Charset charset) throws IOException {
        this.fsDataOutputStream = fileSystem.create(path);
        Out_path = path;
        Out_Pathstr = Out_path.toString();
        this.charset = charset;
    }

    /**
     * 使用默认字符集构造函数  Use default charset constructor
     *
     * @param fileSystem HDFS对象，会通过该对象获取HDFS集群地址
     *                   <p>
     *                   HDFS object, through which the HDFS cluster address will be obtained
     * @param path       数据输出路径  data output path
     * @throws IOException 无法通过HDFS对象获取目标HDFS 或 Path错误的时候，都有可能抛出该错误
     *                     <p>
     *                     This error may be thrown when the target HDFS or Path error cannot be obtained through the HDFS object
     */
    public HDFSWriter(FileSystem fileSystem, Path path) throws IOException {
        this.fsDataOutputStream = fileSystem.create(path);
        Out_path = path;
        Out_Pathstr = Out_path.toString();
    }

    /**
     * @return HDFS的DataTear文件输出组件的建造者对象
     * <p>
     * Builder object for HDFS's Data Tear file output component
     */
    public static HDFSWriterBuilder builder() {
        return new HDFSWriterBuilder();
    }

    @Override
    public void write(byte[] b) throws IOException {
        fsDataOutputStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        fsDataOutputStream.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        fsDataOutputStream.flush();
    }

    @Override
    public void close() throws IOException {
        fsDataOutputStream.close();
    }

    @Override
    public void write(int b) throws IOException {
        fsDataOutputStream.write(b);
    }

    @Override
    public String getPath() {
        return Out_Pathstr;
    }

    @Override
    public Writer toToObject() {
        return this;
    }
}
