package zhao.io.dataTear.dataOp.dataTearRW.hdfs;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import zhao.io.dataTear.dataOp.dataTearRW.Reader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

/**
 * 在HDFS中使用GZIP算法进行数据解码读取的组件，通过此组件您可以直接读取HDFS中的GZIP数据
 * <p>
 * A component that uses the GZIP algorithm to decode and read data in HDFS. With this component, you can directly read GZIP data in HDFS.
 */
public class HDFSReaderGZIP extends Reader {
    protected final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    protected Path In_path;
    protected String In_PathStr;

    public HDFSReaderGZIP(FileSystem fileSystem, Path in_path, String in_PathStr) {
        try {
            setInputStream(fileSystem.open(in_path));
            this.In_path = in_path;
            this.In_PathStr = in_PathStr;
        } catch (IOException e) {
            logger.error("组件：" + this.getClass().getName() + " 启动数据流时出现异常！目标数据：" + in_PathStr + ",错误原因：" + e);
            e.printStackTrace(System.err);
        }
    }

    public static HDFSReaderGZIPBuilder builder() {
        return new HDFSReaderGZIPBuilder();
    }

    @Override
    public boolean closeStream() throws IOException {
        return super.closeStream();
    }

    @Override
    public boolean openStream() {
        return getInputStream() != null;
    }

    @Override
    public int read() throws IOException {
        op_Data();
        return -1;
    }

    @Override
    public boolean op_Data() {
        try {
            IOUtils.copy(getInputStream(), byteArrayOutputStream);
            return true;
        } catch (IOException e) {
            return false;
        } finally {
            try {
                byteArrayOutputStream.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                byteArrayOutputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * @param data 需要解压的数组 Array to be decompressed
     * @return 解压之后的数组  Array after decompression
     */
    private byte[] unGZip(byte[] data) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(data);
            GZIPInputStream gzipInputStream = new GZIPInputStream(bis);
            IOUtils.copy(gzipInputStream, byteArrayOutputStream);
            gzipInputStream.close();
            bis.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                byteArrayOutputStream.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                byteArrayOutputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return byteArrayOutputStream.toString().trim().getBytes();
    }

    public int available() throws IOException {
        return getInputStream().available();
    }

    @Override
    public byte[] getDataArray() {
        setByteArray(unGZip(byteArrayOutputStream.toByteArray()));
        return super.getDataArray();
    }

    public void close() {
        try {
            closeStream();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
