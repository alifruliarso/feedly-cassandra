package com.feedly.cassandra.bean.enhance;

import java.util.BitSet;

import org.objectweb.asm.Type;

import feedly.net.sf.cglib.core.CodeEmitter;
import feedly.net.sf.cglib.core.Constants;
import feedly.net.sf.cglib.core.EmitUtils;
import feedly.net.sf.cglib.core.ReflectUtils;
import feedly.net.sf.cglib.core.Signature;
import feedly.net.sf.cglib.transform.ClassEmitterTransformer;

public class ColumnPropertyEnhancer extends ClassEmitterTransformer
{
    private final BeanInfoVisitor _info;
    private boolean _accessorAdded = false;
    public ColumnPropertyEnhancer(BeanInfoVisitor info)
    {
        _info = info;
    }
    
    
    @Override
    public CodeEmitter begin_method(int access, Signature sig, Type[] exceptions)
    {
        if(!_accessorAdded)
        {
            _accessorAdded = true;
            declare_field(Constants.ACC_PRIVATE, "__modifiedFields", Type.getType(BitSet.class), null);
            declare_field(Constants.ACC_PRIVATE, "__unmappedModified", Type.getType(boolean.class), null);
            EmitUtils.add_property(this, "modifiedFields", Type.getType(BitSet.class), "__modifiedFields");
            EmitUtils.add_property(this, "unmappedFieldsModified", Type.getType(boolean.class), "__unmappedModified");
        }

        String field = sig.getName();
        field = Character.toLowerCase(field.charAt(3)) + field.substring(4);
        final int idx = _info.getColumnFields().indexOf(field);
        final boolean isUnmappedField = field.equals(_info.getUnmappedField());
        if(idx < 0 && !isUnmappedField) 
            return super.begin_method(access, sig, exceptions);
                    
        final CodeEmitter emitter = super.begin_method(access, sig, exceptions);
        return new CodeEmitter(emitter)
        {
            @Override
            public void visitInsn(int opcode)
            {
                if (opcode == Constants.RETURN || opcode == Constants.ARETURN)
                {
                    if(idx >= 0)
                    {
                        load_this();
                        getfield("__modifiedFields");
                        push(idx);
                        try
                        {
                            invoke(ReflectUtils.getMethodInfo(BitSet.class.getMethod("set", int.class)));
                        }
                        catch(Exception ex)
                        {
                            throw new RuntimeException(ex);
                        }
                    }
                    else if(isUnmappedField)
                    {
                        load_this();
                        push(true);
                        putfield("__unmappedModified");
                    }
                }
                super.visitInsn(opcode);
            }
        };
    }
}
