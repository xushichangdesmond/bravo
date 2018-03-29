package powerdancer.bravo.example;

import avro.shaded.com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import powerdancer.bravo.BravoData;

import java.util.Map;

// we encode Record of Class Student with getter overrides for specific fields in its schema.
public class RecordEncoding_WithFieldOverrides {

    static class Student {
        final Scores scores;
        final String name;

        public Student(Scores scores, String name) {
            this.scores = scores;
            this.name = name;
        }
    }

    static class Scores {
        final int mathScore;
        final int javaScore;
        final String name = "meow";

        public Scores(int mathScore, int javaScore) {
            this.mathScore = mathScore;
            this.javaScore = javaScore;
        }
    }

    public static void main(String[] args) throws Throwable {
        Schema schema = SchemaBuilder.record("Student")
                .fields()
                    .requiredLong("serializationTime") //  time of serialization, epoch msec
                    .requiredString("name")
                    .name("scores").type(
                            SchemaBuilder.record("Scores")
                                    .fields()
                                        .requiredInt("mathScore")
                                        .requiredInt("javaScore")
                                        .requiredInt("total") // sum of mathScore and javaScore
                                        .requiredString("name")
                                    .endRecord()
                    ).noDefault()
                .endRecord();


        Map<String, BravoData.FieldAccessor> overrides = ImmutableMap.of(
                "serializationTime",
                BravoData.FieldAccessor.readOnly(rec->System.currentTimeMillis()),
                "total",
                BravoData.FieldAccessor.<Scores, Integer>readOnly(rec->rec.mathScore + rec.javaScore)
        );

        BravoData bravoData = BravoData.buildWithOverrides(
                (recordClass, recordSchema, fieldName) -> overrides.get(fieldName)
        );

        JsonEncoder enc = EncoderFactory.get().jsonEncoder(schema, System.out, true);

        DatumWriter<Student> datumWriter = bravoData.createDatumWriter(schema);

        datumWriter.write(new Student(new Scores(35,30), "dancer"), enc);
        enc.flush();

    }
}
