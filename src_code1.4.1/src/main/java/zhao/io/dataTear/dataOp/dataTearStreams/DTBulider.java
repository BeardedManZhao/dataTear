package zhao.io.dataTear.dataOp.dataTearStreams;

import zhao.io.dataTear.atzhaoPublic.Builder;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

public class DTBulider implements Builder<DTWrite> {
    private String path;

    /**
     * @param path 指定 DT的NameManager文件路径
     * @return 链
     */
    public DTBulider setPath(String path) {
        this.path = path;
        return this;
    }

    @Override
    public DTWrite create() {
        try {
            return new DTWrite(new BufferedOutputStream(new FileOutputStream(path)), path);
        } catch (FileNotFoundException e) {
            e.printStackTrace(System.err);
            return null;
        }
    }
}
