package zhao.io.dataTear.dataOp.dataTearRW;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zhao.io.dataTear.atzhaoPublic.Product;

import java.io.OutputStream;

/**
 * @author 赵凌宇
 * 输出组件 通过输出组件的规则将按照DataTear的格式输出
 * 此接口会被系统内部自动调用，无需实现。
 */
public abstract class Writer extends OutputStream implements Product<Writer> {
    protected final static Logger logger = LoggerFactory.getLogger("DataTear_WriteUDFStream");

    public abstract String getPath();
}
