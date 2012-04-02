package com.feedly.cassandra.entity.enhance;

import static org.objectweb.asm.Opcodes.*;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.AnnotationNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.FieldNode;
import org.objectweb.asm.tree.InsnList;
import org.objectweb.asm.tree.InsnNode;
import org.objectweb.asm.tree.IntInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.TypeInsnNode;
import org.objectweb.asm.tree.VarInsnNode;

import com.feedly.cassandra.anno.Column;
import com.feedly.cassandra.anno.ColumnFamily;
import com.feedly.cassandra.anno.EmbeddedEntity;
import com.feedly.cassandra.anno.UnmappedColumnHandler;
import com.feedly.cassandra.entity.PropertyMetadataFactory;

public class EntityTransformer extends ClassTransformer
{
    
    public EntityTransformer(ClassTransformer ct)
    {
        super(ct);
    }

    @Override
    public boolean transform(ClassNode cn)
    {
        boolean rv = addInterface(cn);
        if(rv)
        {
            addFields(cn);
            implementInterface(cn);
            modifyConstructor(cn);
            modifyUnmappedHandler(cn);
            try
            {
                /* int cnt = */ modifyAccessors(cn);
//                System.out.println(cn.name + ": " + cnt + " properties detected");
            }
            catch(ClassNotFoundException cnfe)
            {
                throw new RuntimeException("entity classes must be valid.", cnfe);
            }
        }
//        else
//            System.out.println("skipping " + cn.name);

        return rv || super.transform(cn);
    }

    private boolean addInterface(ClassNode cn)
    {
        String iface = Type.getInternalName(IEnhancedEntity.class);
        if(cn.interfaces.contains(iface))
            return false;
        
        String annoType = Type.getDescriptor(ColumnFamily.class);
        String annoType2 = Type.getDescriptor(EmbeddedEntity.class);
        boolean hasAnno = false;
        
        if(cn.visibleAnnotations != null)
        {
            for(AnnotationNode anno : cn.visibleAnnotations)
            {
                if(anno.desc.equals(annoType) || anno.desc.equals(annoType2))
                {
                    hasAnno = true;
                    break;
                }
            }
        }
        
        if(!hasAnno)
            return false;
        
        cn.interfaces.add(iface);
        return true;
    }

    private void implementInterface(ClassNode cn)
    {
        /*
         *     public BitSet getModifiedFields();
         *     public void setModifiedFields(BitSet b);
         *     
         *     public boolean getUnmappedFieldsModified();
         *     public void setUnmappedFieldsModified(boolean b);
         */
        addAccessors(cn, "__modifiedFields", "ModifiedFields", BitSet.class);
        addAccessors(cn, "__unmappedModified", "UnmappedFieldsModified", boolean.class);
    }

    private void addAccessors(ClassNode cn, String propName, String methodName, Class<?> type)
    {
        MethodNode mn = new MethodNode(ACC_PUBLIC, "get" + methodName, "()" + Type.getType(type).getDescriptor(), null, null);
        mn.instructions.add(new VarInsnNode(ALOAD, 0));
        mn.instructions.add(new FieldInsnNode(GETFIELD, cn.name, propName, Type.getDescriptor(type)));
        mn.instructions.add(new InsnNode(type.isPrimitive() ? IRETURN : ARETURN));
        mn.maxLocals = 1;
        mn.maxStack = 1;
        cn.methods.add(mn);

        mn = new MethodNode(ACC_PUBLIC, "set" + methodName, "(" + Type.getType(type).getDescriptor() + ")" + Type.getType(void.class), null, null);
        mn.instructions.add(new VarInsnNode(ALOAD, 0));
        mn.instructions.add(new VarInsnNode(type.isPrimitive() ? ILOAD : ALOAD, 1));
        mn.instructions.add(new FieldInsnNode(PUTFIELD, cn.name, propName, Type.getDescriptor(type)));
        mn.instructions.add(new InsnNode(RETURN));
        mn.maxLocals = 2;
        mn.maxStack = 2;

        cn.methods.add(mn);
    }

    private void addFields(ClassNode cn)
    {
        int acc = ACC_PRIVATE;
        cn.fields.add(new FieldNode(acc, "__modifiedFields", Type.getDescriptor(BitSet.class), null, null));
        cn.fields.add(new FieldNode(acc, "__unmappedModified", Type.getDescriptor(boolean.class), null, null));
    }

    private void modifyConstructor(ClassNode cn)
    {
        for(MethodNode mn : cn.methods)
        {
            if(!"<init>".equals(mn.name))
                continue;
            
            InsnList insns = mn.instructions;
            if(insns.size() == 0)
                continue;
            
            Iterator<AbstractInsnNode> j = insns.iterator();
            while(j.hasNext())
            {
                AbstractInsnNode in = j.next();
                int op = in.getOpcode();
                if(op == RETURN)
                {
                    InsnList il = new InsnList();
                    il.add(new VarInsnNode(ALOAD, 0));
                    il.add(new TypeInsnNode(NEW, Type.getInternalName(BitSet.class)));
                    il.add(new InsnNode(DUP));
                    il.add(new MethodInsnNode(INVOKESPECIAL, Type.getInternalName(BitSet.class), "<init>", "()V"));
                    il.add(new FieldInsnNode(PUTFIELD, cn.name, "__modifiedFields", Type.getDescriptor(BitSet.class)));
                    insns.insert(in.getPrevious(), il);
                }
            }
            
            mn.maxStack += 2;
        }
    }

    private void modifyUnmappedHandler(ClassNode cn)
    {
        String desc = Type.getDescriptor(UnmappedColumnHandler.class);
        String fieldName = null;
        
        for(FieldNode field : cn.fields)
        {
            if(field.visibleAnnotations != null)
            {
                for(AnnotationNode anno : field.visibleAnnotations)
                {
                    if(anno.desc.equals(desc))
                    {
                        fieldName = field.name;
                        break;
                    }
                }
            }
        }
        
        if(fieldName != null)
        {
            String setterName = accessorName(fieldName, true);
            String getterName = accessorName(fieldName, false);

            for(MethodNode mn : cn.methods)
            {
                if(mn.name.equals(setterName) || mn.name.equals(getterName))
                {
                    insertUnmappedModBitInsns(cn, mn);
                    if(mn.name.equals(getterName))
                        mn.maxStack++;
                }
            }
            
        }
        
    }

    private void insertUnmappedModBitInsns(ClassNode cn, MethodNode mn)
    {
        Iterator<AbstractInsnNode> j = mn.instructions.iterator();
        while(j.hasNext())
        {
            AbstractInsnNode in = j.next();
            int op = in.getOpcode();
            if(op >= IRETURN && op <= RETURN)
            {
                InsnList il = new InsnList();
                il.add(new VarInsnNode(ALOAD, 0));
                il.add(new InsnNode(ICONST_1));
                il.add(new FieldInsnNode(PUTFIELD, cn.name, "__unmappedModified", Type.getDescriptor(boolean.class)));

                mn.instructions.insert(in.getPrevious(), il);
                break;
            }
        }
        
        mn.maxLocals++;
        mn.maxStack++;
    }

    private int modifyAccessors(ClassNode cn) throws ClassNotFoundException
    {
        String desc = Type.getDescriptor(Column.class);
        List<String> fieldNames = new ArrayList<String>();
        for(FieldNode field : cn.fields)
        {
            if(field.visibleAnnotations != null)
            {
                for(AnnotationNode anno : field.visibleAnnotations)
                {
                    if(anno.desc.equals(desc))
                    {
                        fieldNames.add(field.name);
                    }
                }
            }
        }
        
        Collections.sort(fieldNames);
        
        for(FieldNode field : cn.fields)
        {
            if(fieldNames.contains(field.name))
            {
                for(AnnotationNode anno : field.visibleAnnotations)
                {
                    if(anno.desc.equals(desc))
                    {
                        String setterName = accessorName(field.name, true);
                        String getterName = accessorName(field.name, false);
                        
                        for(MethodNode mn : cn.methods)
                        {
                            boolean isSimple = false;
                            
                            if(!isSimple)
                                isSimple = PropertyMetadataFactory.isPrimitiveType(Type.getType(field.desc).getClassName());
                            
                            if(!isSimple)
                                isSimple = PropertyMetadataFactory.isSimpleType(Class.forName(Type.getType(field.desc).getClassName()));
                            
                            if(mn.name.equals(setterName) || (mn.name.equals(getterName) && !isSimple))
                            {
                                insertModBitInsns(cn, mn, fieldNames.indexOf(field.name));
                                if(mn.name.equals(getterName))
                                    mn.maxStack++;
                            }
                        }
                    }
                }
            }
        }
        
        return fieldNames.size();
    }


    private void insertModBitInsns(ClassNode cn, MethodNode mn, int bitPos)
    {
        Iterator<AbstractInsnNode> j = mn.instructions.iterator();
        while(j.hasNext())
        {
            AbstractInsnNode in = j.next();
            int op = in.getOpcode();
            if(op >= IRETURN && op <= RETURN)
            {
                InsnList il = new InsnList();
                il.add(new VarInsnNode(ALOAD, 0));
                il.add(new FieldInsnNode(GETFIELD, cn.name, "__modifiedFields", Type.getDescriptor(BitSet.class)));
                
                if(bitPos <= 5)
                    il.add(new InsnNode(ICONST_0 + bitPos));
                else
                    il.add(new IntInsnNode(BIPUSH, bitPos));

                il.add(new MethodInsnNode(INVOKEVIRTUAL, Type.getInternalName(BitSet.class), "set", "(I)V"));
                mn.instructions.insert(in.getPrevious(), il);
                break;
            }
        }
        
        mn.maxLocals++;
        mn.maxStack++;
    }

    private String accessorName(String fieldName, boolean setter)
    {
        StringBuilder b = new StringBuilder(setter ? "set" : "get");
        b.append(Character.toUpperCase(fieldName.charAt(0)));
        if(fieldName.length() > 1)
            b.append(fieldName.substring(1));
        
        return b.toString(); 
    }
}
