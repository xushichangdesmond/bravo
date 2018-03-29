package powerdancer.bravo;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by powerDancer on 1/2/2018.
 */
public class BravoData extends ReflectData {

    protected final ConcurrentMap<ClassAndSchema, RecordDataState> recordDataCache = new ConcurrentHashMap<>();
    protected final SchemaFieldAccessorOverridesProvider schemaFieldAccessorOverridesProvider;

    public BravoData(SchemaFieldAccessorOverridesProvider schemaFieldAccessorOverridesProvider) {
        this.schemaFieldAccessorOverridesProvider = Objects.requireNonNull(schemaFieldAccessorOverridesProvider);
    }

    @Override
    protected RecordDataState getRecordState(Object record, Schema schema) {
        return recordDataCache.computeIfAbsent(new ClassAndSchema(record.getClass(), schema),
                p -> new RecordDataState(
                        super.getRecordState(record, schema),
                        schemaFieldAccessorOverridesProvider.apply(record.getClass(), schema)
                )
        );
    }

    @Override
    protected Object getField(Object record, String name, int pos, Object state) {
        if (!(state instanceof RecordDataState)) {
            try {
                return super.getField(record, name, pos, state);
            }
            catch(NullPointerException e) {
                return null;
            }
        }
        RecordDataState ds = ((RecordDataState) state);
        FieldAccessor acc = ds.fields[pos];
        return (acc == null) ?
                super.getField(record, name, pos, ds.reflectState)
                :
                acc.get(record);
    }

    @Override
    protected void setField(Object record, String name, int pos, Object o, Object state) {
        RecordDataState ds = ((RecordDataState) state);
        FieldAccessor acc = ds.fields[pos];
        if (acc == null) {
            super.setField(record, name, pos, o, ds.reflectState);
        } else {
            acc.set(record, o);
        }
    }

    @FunctionalInterface
    public interface SchemaFieldAccessorOverridesProvider extends BiFunction<Class, Schema, FieldAccessor[]> {
        @Override
        FieldAccessor[] apply(Class recordClass, Schema recordSchema);
    }

    @FunctionalInterface
    public interface FieldAccessorOverridesProvider {
        FieldAccessor apply(Class recordClass, Schema recordSchema, String fieldName);
    }

    public static class RecordDataState {
        final Object reflectState;
        final FieldAccessor[] fields;

        public RecordDataState(Object reflectState, FieldAccessor[] fields) {
            this.reflectState = reflectState;
            this.fields = fields;
        }
    }

    public interface FieldAccessor {
        Object get(Object record);

        void set(Object record, Object val);

        static <T, R> FieldAccessor readOnly(Function<T, R> getter) {
            return new FieldAccessor() {
                @Override
                public Object get(Object record) {
                    return getter.apply((T) record);
                }

                @Override
                public void set(Object record, Object val) {

                }
            };
        }

    }

    public static BravoData buildWithOverrides(FieldAccessorOverridesProvider p) {
        return new BravoData((recordClass, recordSchema) -> {
            return recordSchema.getFields().stream().map(
                    f->p.apply(recordClass, recordSchema, f.name())
            ).toArray(FieldAccessor[]::new);
        });
    }

    @Override
    public DatumWriter createDatumWriter(Schema schema) {
        return new ReflectDatumWriter(schema, this) {
            @Override
            protected void writeField(Object record, Schema.Field f, Encoder out, Object state) throws IOException {
                RecordDataState ds = ((RecordDataState) state);
                FieldAccessor acc = ds.fields[f.pos()];
                if (acc == null) {
                    super.writeField(record, f, out, ds.reflectState);
                }
                else {
                    super.write(f.schema(), acc.get(record), out);
                }
            }
        };
    }

    public class ClassAndSchema {
        public final Class klass;
        public final Schema schema;

        public ClassAndSchema(Class klass, Schema schema) {
            this.klass = klass;
            this.schema = schema;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ClassAndSchema that = (ClassAndSchema) o;
            return Objects.equals(klass, that.klass) &&
                    Objects.equals(schema, that.schema);
        }

        @Override
        public int hashCode() {
            return klass.hashCode() * 31 + schema.hashCode();
        }
    }
}
