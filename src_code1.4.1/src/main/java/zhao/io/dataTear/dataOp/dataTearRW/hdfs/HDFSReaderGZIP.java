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
 * TODO 需要更新GZIP算法的输入流组件
 */
public class HDFSReaderGZIP extends Reader {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    Path In_path;
    String In_Pathstr;

    public HDFSReaderGZIP(FileSystem fileSystem, Path in_path, String in_Pathstr) {
        try {
            setInputStream(fileSystem.open(in_path));
            In_path = in_path;
            In_Pathstr = in_Pathstr;
        } catch (IOException e) {
            logger.error("组件：" + this.getClass().getName() + " 启动数据流时出现异常！目标数据：" + in_Pathstr + ",错误原因：" + e);
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
     * @param data 需要解压的数组
     * @return 解压之后的数组
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
