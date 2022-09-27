package zhao.io.dataTear.dataContainer;

import java.io.Serializable;

/**
 * 数据接口，本文件存储格式的所有数据都应该实现此类
 * <p>
 * Data interface, all data in this file storage format should implement this class
 *
 * @param <T> 数据类型
 */
public interface RWData<T> extends Serializable {

    void putData(T data);

    /**
     * @return 需要被文件存储格式操作的对象  Objects that need to be manipulated by the file storage format
     */
    T getData();
}
