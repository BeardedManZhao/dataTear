package zhao.io.dataTear.dataOp.dataTearRW.hdfs;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import zhao.io.dataTear.dataOp.dataTearRW.Writer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.zip.GZIPOutputStream;

/**
 * @author 赵凌宇
 * HDFS中构建GZIP_DT目录的数据输出组件
 */
public class HDFSWriterGZIP extends Writer {

    protected final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    protected final ByteArrayOutputStream OKYA = new ByteArrayOutputStream();
    protected FSDataOutputStream fsDataOutputStream;
    Path Out_path;
    String Out_PathStr;
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
    public HDFSWriterGZIP(FileSystem fileSystem, Path path, Charset charset) throws IOException {
        this.fsDataOutputStream = fileSystem.create(path);
        Out_path = path;
        Out_PathStr = Out_path.toString();
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
    public HDFSWriterGZIP(FileSystem fileSystem, Path path) throws IOException {
        this.fsDataOutputStream = fileSystem.create(path);
        Out_path = path;
        Out_PathStr = Out_path.toString();
    }


    public static HDFSWriterGZIPBuilder builder() {
        return new HDFSWriterGZIPBuilder();
    }

    @Override
    public void write(byte[] b) throws IOException {
        byteArrayOutputStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        byteArrayOutputStream.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        byteArrayOutputStream.flush();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(OKYA);
        gzipOutputStream.write(byteArrayOutputStream.toByteArray());
        gzipOutputStream.flush();
        gzipOutputStream.close();
        fsDataOutputStream.write(OKYA.toByteArray());
    }

    @Override
    public void close() throws IOException {
        OKYA.flush();
        OKYA.close();
        byteArrayOutputStream.close();
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
    }

    @Override
    public String getPath() {
        return this.Out_PathStr;
    }

    @Override
    public void write(int b) throws IOException {
        byteArrayOutputStream.write(b);
    }

    @Override
    public Writer toTobject() {
        return this;
    }
}
