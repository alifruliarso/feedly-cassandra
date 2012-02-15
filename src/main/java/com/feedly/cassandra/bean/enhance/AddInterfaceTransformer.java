package com.feedly.cassandra.bean.enhance;

import org.objectweb.asm.Type;

import feedly.net.sf.cglib.transform.ClassEmitterTransformer;

public class AddInterfaceTransformer extends ClassEmitterTransformer
{
    public Class<?>[] _ifaces;
    
    public AddInterfaceTransformer(Class<?>...classes)
    {
        _ifaces = classes;
    }
    

    @Override
    public void begin_class(int version, int access, String className, Type superType, Type[] interfaces, String source)
    {
        Type[] all = new Type[interfaces.length + 1];
        System.arraycopy(interfaces, 0, all, 0, interfaces.length);
        all[all.length - 1] = Type.getType(IEnhancedBean.class);
        super.begin_class(version, access, className, superType, all, source);
    }
}
