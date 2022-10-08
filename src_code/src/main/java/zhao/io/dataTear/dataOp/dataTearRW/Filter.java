package zhao.io.dataTear.dataOp.dataTearRW;

/**
 * 过滤接口，用于实现过滤的lambda操作。
 * <p>
 * Filter interface, used to implement filtering lambda operations.
 */
public interface Filter {
    /**
     * 判断数据是否满足条件的接口。
     * <p>
     * An interface for judging whether the data meets the conditions.
     *
     * @param data 被判断数据
     *             <p>
     *             judged data.
     * @return 判断结果 为true 代表满足条件
     * <p>
     * The judgment result is true, which means that the condition is met
     */
    boolean filter(String data);
}
