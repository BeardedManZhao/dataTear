package zhao.io.dataTear.dataOp.dataTearRW.hdfs;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import zhao.io.dataTear.dataOp.dataTearRW.Writer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * @author 赵凌宇
 * HDFS输出ZIP算法的DT目录构造组件
 */
public class HDFSWriterZIP extends Writer {
    protected final ByteArrayOutputStream noya = new ByteArrayOutputStream();
    protected final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    protected FSDataOutputStream fsDataOutputStream;
    Path Out_path;
    String Out_PathStr;

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
    public HDFSWriterZIP(FileSystem fileSystem, Path path, String charset) throws IOException {
        this.fsDataOutputStream = fileSystem.create(path);
        Out_path = path;
        Out_PathStr = Out_path.toString();
    }

    /**
     * @return HDFS的DataTear文件输出组件的建造者对象
     * <p>
     * Builder object for HDFS's Data Tear file output component
     */
    public static HDFSWriterZIPBuilder builder() {
        return new HDFSWriterZIPBuilder();
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        noya.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        noya.flush();
        ZipOutputStream zipOutputStream = new ZipOutputStream(byteArrayOutputStream);
        ZipEntry zipEntry = new ZipEntry("DT-ZHAO");
        zipOutputStream.putNextEntry(zipEntry);
        zipOutputStream.write(noya.toByteArray());
        zipOutputStream.flush();
        zipOutputStream.closeEntry();
        zipOutputStream.close();
        fsDataOutputStream.write(byteArrayOutputStream.toByteArray());
        byteArrayOutputStream.flush();
        fsDataOutputStream.flush();
    }

    @Override
    public void close() throws IOException {
        noya.close();
        byteArrayOutputStream.close();
        fsDataOutputStream.close();
    }

    @Override
    public void write(int b) throws IOException {
        noya.write(b);
    }

    @Override
    public String getPath() {
        return Out_PathStr;
    }

    @Override
    public Writer toTobject() {
        return this;
    }

    @Override
    public void write(byte[] b) throws IOException {
        noya.write(b);
    }
}
