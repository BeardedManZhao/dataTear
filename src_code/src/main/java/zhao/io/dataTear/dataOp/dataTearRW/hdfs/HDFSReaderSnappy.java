package zhao.io.dataTear.dataOp.dataTearRW.hdfs;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.xerial.snappy.SnappyInputStream;
import zhao.io.dataTear.dataOp.dataTearRW.Reader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Snappy算法的输入流组件，您可以通过这个组件读取HDFS中的Snappy数据
 * <p>
 * The input stream component of the Snappy algorithm, you can read the Snappy data in HDFS through this component.
 */
public class HDFSReaderSnappy extends Reader {
    protected final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    protected Path In_path;
    protected String In_PathStr;

    public HDFSReaderSnappy(FileSystem fileSystem, Path in_path, String in_PathStr) {
        try {
            setInputStream(fileSystem.open(in_path));
            In_path = in_path;
            In_PathStr = in_PathStr;
        } catch (IOException e) {
            logger.error("组件：" + this.getClass().getName() + " 启动数据流时出现异常！目标数据：" + in_PathStr + ",错误原因：" + e);
            e.printStackTrace(System.err);
        }
    }

    public static HDFSReaderSnappyBuilder builder() {
        return new HDFSReaderSnappyBuilder();
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
     * @param data 需要解压的数组  Array to be decompressed
     * @return 解压之后的数组  Array after decompression
     */
    private byte[] unSnappy(byte[] data) {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            SnappyInputStream framedSnappyCompressorInputStream = new SnappyInputStream(byteArrayInputStream);
            IOUtils.copy(framedSnappyCompressorInputStream, byteArrayOutputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return byteArrayOutputStream.toString().trim().getBytes();
    }

    public int available() throws IOException {
        return getInputStream().available();
    }

    @Override
    public byte[] getDataArray() {
        setByteArray(unSnappy(byteArrayOutputStream.toByteArray()));
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
