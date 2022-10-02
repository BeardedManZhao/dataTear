package zhao.io.dataTear.dataOp.dataTearRW.hdfs;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.xerial.snappy.SnappyOutputStream;
import zhao.io.dataTear.dataOp.dataTearRW.Writer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * @author 赵凌宇
 * HDFS中构建Snappy_DT目录的数据输出组件
 */
public class HDFSWriterSnappy extends Writer {

    FSDataOutputStream fsDataOutputStream;
    Path Out_path;
    String Out_Pathstr;
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    ByteArrayOutputStream OKYA = new ByteArrayOutputStream();
    private Charset charset;

    /**
     * 由建造者进行构造函数的执行，获取类
     *
     * @param fileSystem HDFS对象，会通过该对象获取HDFS集群地址
     * @param path       数据输出路径
     * @param charset    数据输出编码集
     * @throws IOException 无法通过HDFS对象获取目标HDFS 或 Path错误的时候，都有可能抛出该错误
     */
    public HDFSWriterSnappy(FileSystem fileSystem, Path path, Charset charset) throws IOException {
        this.fsDataOutputStream = fileSystem.create(path);
        Out_path = path;
        Out_Pathstr = Out_path.toString();
        this.charset = charset;
    }

    /**
     * 使用默认字符集构造函数
     *
     * @param fileSystem HDFS对象，会通过该对象获取HDFS集群地址
     * @param path       数据输出路径
     * @throws IOException 无法通过HDFS对象获取目标HDFS 或 Path错误的时候，都有可能抛出该错误
     */
    public HDFSWriterSnappy(FileSystem fileSystem, Path path) throws IOException {
        this.fsDataOutputStream = fileSystem.create(path);
        Out_path = path;
        Out_Pathstr = Out_path.toString();
    }


    public static HDFSWriterSnappyBuilder builder() {
        return new HDFSWriterSnappyBuilder();
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
        SnappyOutputStream snappyOutputStream = new SnappyOutputStream(OKYA);
        snappyOutputStream.write(byteArrayOutputStream.toByteArray());
        snappyOutputStream.flush();
        snappyOutputStream.close();
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
        return this.Out_Pathstr;
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
