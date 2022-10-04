package zhao.io.dataTear.dataOp.dataTearRW.hdfs;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import zhao.io.dataTear.dataOp.dataTearRW.Reader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * HDFS数据读取组件，使用BZIP2算法进行数据解码读取，与BZIP2数据的数据输出是相互对应的
 * <p>
 * The HDFS data reading component uses the BZIP 2 algorithm for data decoding and reading, which corresponds to the data output of the BZIP 2 data.
 *
 * @see zhao.io.dataTear.dataOp.dataTearRW.hdfs.HDFSWriterBZIP2
 */
public class HDFSReaderBZIP2 extends Reader {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    Path In_path;
    String In_Pathstr;

    public HDFSReaderBZIP2(FileSystem fileSystem, Path in_path, String in_Pathstr) {
        try {
            setInputStream(fileSystem.open(in_path));
            In_path = in_path;
            In_Pathstr = in_Pathstr;
        } catch (IOException e) {
            logger.error("组件：" + this.getClass().getName() + " 启动数据流时出现异常！目标数据：" + in_Pathstr + ",错误原因：" + e);
            e.printStackTrace(System.err);
        }
    }

    public static HDFSReaderBZIP2Builder builder() {
        return new HDFSReaderBZIP2Builder();
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
    private byte[] unBZIP2(byte[] data) {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            BZip2CompressorInputStream bZip2CompressorInputStream = new BZip2CompressorInputStream(byteArrayInputStream);
            IOUtils.copy(bZip2CompressorInputStream, byteArrayOutputStream);
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
        setByteArray(unBZIP2(byteArrayOutputStream.toByteArray()));
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
