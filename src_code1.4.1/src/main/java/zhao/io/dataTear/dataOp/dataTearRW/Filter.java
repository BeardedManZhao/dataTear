package zhao.io.dataTear.dataOp.dataTearRW;

/**
 * 过滤接口
 */
public interface Filter {
    /**
     * 判断数据是否满足条件的接口
     *
     * @param data 被判断数据
     * @return 判断结果 为true 代表满足条件
     */
    boolean filter(String data);
}
