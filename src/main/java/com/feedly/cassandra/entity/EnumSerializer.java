package com.feedly.cassandra.entity;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;

import org.apache.cassandra.db.migration.avro.IndexType;

public class EnumSerializer extends AbstractSerializer<Enum<?>>
{
    private final StringSerializer _ser = StringSerializer.get();
    private final Class<? extends Enum<?>> _clazz;
    private final Method _valueOf;
    
    public EnumSerializer(Class<? extends Enum<?>> enumClass)
    {
        _clazz = enumClass;
        try
        {
            _valueOf = _clazz.getMethod("valueOf", String.class);
        }
        catch(NoSuchMethodException ex)
        {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public ByteBuffer toByteBuffer(Enum<?> obj)
    {
        return _ser.toByteBuffer(obj.name());
    }

    @Override
    public Enum<?> fromByteBuffer(ByteBuffer byteBuffer)
    {
        String name = _ser.fromByteBuffer(byteBuffer);
        if(name == null || "".equals(name))
            return null;
        
        try
        {
            return (Enum<?>) _valueOf.invoke(null, name);
        }
        catch(Exception ex)
        {
            throw new IllegalStateException(ex);
        }
    }
    
    public static void main(String[] args) throws Throwable
    {
        EnumSerializer ser = new EnumSerializer(IndexType.class);
        byte[] bytes = ser.toBytes(IndexType.CUSTOM);
        
        System.out.println(Arrays.toString(bytes));
        System.out.println(ser.fromBytes(bytes));
    }
}
