package zhao.io.dataTear.dataOp.dataTearRW.hdfs;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import zhao.io.dataTear.dataOp.dataTearRW.Reader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.ZipInputStream;

public class HDFSReaderZIP extends Reader {
    protected final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    Path In_path;
    String In_PathStr;

    public HDFSReaderZIP(FileSystem fileSystem, Path in_path, String in_PathStr, String charset) {
        try {
            setInputStream(fileSystem.open(in_path));
            In_path = in_path;
            In_PathStr = in_PathStr;
        } catch (IOException e) {
            logger.error("组件：" + this.getClass().getName() + " 启动数据流时出现异常！目标数据：" + in_PathStr + ",错误原因：" + e);
            e.printStackTrace(System.err);
        }
    }

    public static HDFSReaderZIPBuilder builder() {
        return new HDFSReaderZIPBuilder();
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
    private byte[] unZip(byte[] data) {
        byte[] b = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(data);
            ZipInputStream zip = new ZipInputStream(bis);
            if (zip.getNextEntry() != null) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                IOUtils.copy(zip, baos);
                b = baos.toByteArray();
                baos.flush();
                baos.close();
            }
            zip.close();
            bis.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return b;
    }

    public int available() throws IOException {
        return getInputStream().available();
    }

    @Override
    public byte[] getDataArray() {
        setByteArray(unZip(byteArrayOutputStream.toByteArray()));
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
