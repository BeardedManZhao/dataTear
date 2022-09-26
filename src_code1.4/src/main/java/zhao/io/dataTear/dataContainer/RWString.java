package zhao.io.dataTear.dataContainer;

/**
 * String数据操作读写容器的拓展类 Extended class for data manipulation read and write containers
 */
public class RWString implements RWData<String> {

    StringBuilder stringBuilder = new StringBuilder();

    @Override
    public void putData(String data) {
        stringBuilder.append(data);
    }

    @Override
    public String getData() {
        return stringBuilder.toString();
    }
}
