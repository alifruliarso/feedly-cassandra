package com.feedly.cassandra.entity.enhance;

import org.objectweb.asm.tree.ClassNode;

public class ClassTransformer
{
    protected ClassTransformer _ct;
    protected boolean _transformed;
    
    
    public ClassTransformer(ClassTransformer ct)
    {
        _ct = ct;
    }

    public boolean transform(ClassNode cn)
    {
        if(_ct != null)
        {
            return _ct.transform(cn);
        }
        
        return false;
    }

}
