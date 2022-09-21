package zhao.io.dataTear.dataContainer;

/**
 * String数据块的类
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
