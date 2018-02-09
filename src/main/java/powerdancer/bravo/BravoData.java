package powerdancer.bravo;

import avro.shaded.com.google.common.cache.CacheBuilder;
import avro.shaded.com.google.common.cache.CacheLoader;
import avro.shaded.com.google.common.cache.LoadingCache;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

/**
 * Created by powerDancer on 1/2/2018.
 */
public class BravoData extends ReflectData {

    final SchemaFieldAccessorOverridesProvider schemaFieldAccessorOverridesProvider;

    public BravoData(SchemaFieldAccessorOverridesProvider schemaFieldAccessorOverridesProvider) {
        this.schemaFieldAccessorOverridesProvider = Objects.requireNonNull(schemaFieldAccessorOverridesProvider);
    }

    @Override
    protected RecordDataState getRecordState(Object record, Schema schema) {
        return new RecordDataState(
                super.getRecordState(record, schema),
                schemaFieldAccessorOverridesProvider.apply(schema)
        );
    }

    @Override
    protected Object getField(Object record, String name, int pos, Object state) {
        if (!(state instanceof RecordDataState)) {
            return super.getField(record, name, pos, state);
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
    public interface SchemaFieldAccessorOverridesProvider extends Function<Schema, FieldAccessor[]> {
    }

    @FunctionalInterface
    public interface FieldAccessorOverridesProvider extends Function<Schema.Field, FieldAccessor> {
    }

    static class RecordDataState {
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
        LoadingCache<Schema, FieldAccessor[]> c = CacheBuilder.newBuilder().build(
                CacheLoader.from(schema -> {
                    return schema.getFields().stream().map(p).toArray(FieldAccessor[]::new);
                })
        );
        return new BravoData(c::getUnchecked);
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
}
