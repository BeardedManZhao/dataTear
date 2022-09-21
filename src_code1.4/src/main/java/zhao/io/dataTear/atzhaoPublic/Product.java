package zhao.io.dataTear.atzhaoPublic;

/**
 * 产品包装接口
 *
 * @param <T> 产品内部类型
 */
public interface Product<T> {
    /**
     * @return 产品内部的被包装类
     */
    T toTobject();
}
