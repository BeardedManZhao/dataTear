package zhao.io.dataTear.dataOp.dataTearStreams;

import zhao.io.dataTear.dataOp.dataTearRW.Reader;
import zhao.io.dataTear.dataOp.dataTearRW.dataBase.DataBaseReader;

import java.io.IOException;
import java.io.OutputStream;

public class DBTEXTDtream implements DT_StreamBase {
    @Override
    public Reader readStream(String TableName) throws IOException {
        return DataBaseReader.builder();
    }

    @Override
    public OutputStream writeStream(String outPath) throws IOException {
        return null;
    }
}
