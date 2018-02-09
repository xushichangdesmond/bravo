package powerdancer.bravo.example;

import avro.shaded.com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import powerdancer.bravo.BravoData;

// we have Class Student having a field of type Scores, but our schema for the Student record is such
// that it contains the fields for its embedded Scores instance directly in a flattened schema of sorts
public class FlattenedSchema {
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

        public Scores(int mathScore, int javaScore) {
            this.mathScore = mathScore;
            this.javaScore = javaScore;
        }
    }

    public static void main(String[] args) throws Throwable {

        Schema schema = SchemaBuilder.record("Student")
                .fields()
                .requiredString("name")
                .requiredInt("mathScore")
                .requiredInt("javaScore")
                .endRecord();

        BravoData bravoData = BravoData.buildWithOverrides(
                ImmutableMap.of(
                        schema.getField("mathScore"),
                        BravoData.FieldAccessor.<Student, Integer>readOnly(rec -> rec.scores.mathScore),
                        schema.getField("javaScore"),
                        BravoData.FieldAccessor.<Student, Integer>readOnly(rec -> rec.scores.javaScore)
                )::get
        );

        JsonEncoder enc = EncoderFactory.get().jsonEncoder(schema, System.out, true);

        DatumWriter<Student> datumWriter = bravoData.createDatumWriter(schema);

        datumWriter.write(new Student(new Scores(35, 30), "dancer"), enc);
        enc.flush();
    }
}
