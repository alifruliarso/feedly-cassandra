package com.feedly.cassandra.entity.enhance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.Attribute;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feedly.cassandra.anno.Column;
import com.feedly.cassandra.anno.ColumnFamily;
import com.feedly.cassandra.anno.UnmappedColumnHandler;

public class BeanInfoVisitor implements ClassVisitor
{
    private static final Logger _logger = LoggerFactory.getLogger(BeanInfoVisitor.class.getName());
    private boolean _alreadyImplementsEnhancer;
    private boolean _hasColFamilyAnnotation;
    private List<String> _columnFields = new ArrayList<String>();
    private Set<String> _columnCollectionFields = new HashSet<String>();
    private String _unmappedField;
    private String _className;

    @Override
    public String toString()
    {
        StringBuilder b = new StringBuilder();
        if(_hasColFamilyAnnotation)
            b.append("@ColumnFamily ");
        
        b.append(_className);
        if(_alreadyImplementsEnhancer)
            b.append(" implements IEnhancer");
        
        b.append(" " + _columnFields);
        
        return b.toString();
    }
    
    public boolean alreadyImplementsEnhancer()
    {
        return _alreadyImplementsEnhancer;
    }

    public boolean hasColFamilyAnnotation()
    {
        return _hasColFamilyAnnotation;
    }

    public List<String> getColumnFields()
    {
        return _columnFields;
    }

    public Set<String> getColumnCollectionFields()
    {
        return _columnCollectionFields;
    }
    
    public String getUnmappedField()
    {
        return _unmappedField;
    }
    
    public String getBeanClassName()
    {
        return _className;
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces)
    {
        _className = name.replaceAll("/", ".");
        String iface = IEnhancedEntity.class.getName().replaceAll("\\.", "/");
        for (String i : interfaces)
        {
            if (iface.equals(i))
            {
                _alreadyImplementsEnhancer = true;
                break;
            }
        }
    }

    private String toJavaSpecName(String className)
    {
        return "L" + className.replaceAll("\\.", "/") + ";";
    }
    
    @Override
    public void visitSource(String source, String debug)
    {
        _logger.trace(source);
    }

    @Override
    public void visitOuterClass(String owner, String name, String desc)
    {
        _logger.trace(owner + ":" + name);
    }

    @Override
    public AnnotationVisitor visitAnnotation(String desc, boolean visible)
    {
        _logger.trace("anno " + desc + ":" + visible);
        
        if(toJavaSpecName(ColumnFamily.class.getName()).equals(desc))
            _hasColFamilyAnnotation = true;
        
        return new EmptyAnnotationVisitor(desc);
    }

    @Override
    public void visitAttribute(Attribute attr)
    {
        _logger.trace(attr.type);
    }

    @Override
    public void visitInnerClass(String name, String outerName, String innerName, int access)
    {

    }

    @Override
    public FieldVisitor visitField(int access, final String name, final String fieldDesc, String signature, Object value)
    {
        _logger.trace("visiting field " + name);
        return new FieldVisitor()
        {

            @Override
            public void visitEnd()
            {
            }

            @Override
            public void visitAttribute(Attribute attr)
            {
                _logger.trace("visit attr " + attr.type);
            }

            @Override
            public AnnotationVisitor visitAnnotation(String desc, boolean visible)
            {
                _logger.trace("anno " + desc + ":" + visible);
                if(toJavaSpecName(Column.class.getName()).equals(desc))
                {
                    _columnFields.add(name);
                    Type type = Type.getType(fieldDesc);
                    if(type.getClassName().equals("java.util.List") || type.getClassName().equals("java.util.Map") || type.getClassName().equals("java.util.SortedMap"))
                        _columnCollectionFields.add(name);
                }

                if(toJavaSpecName(UnmappedColumnHandler.class.getName()).equals(desc))
                {
                    _unmappedField = name;
                }
                return new EmptyAnnotationVisitor(desc);
            }
        };
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions)
    {
        _logger.trace("visiting " + name);
        return new MethodVisitor()
        {

            @Override
            public void visitVarInsn(int opcode, int var)
            {
            }

            @Override
            public void visitTypeInsn(int opcode, String type)
            {
            }

            @Override
            public void visitTryCatchBlock(Label start, Label end, Label handler, String type)
            {
            }

            @Override
            public void visitTableSwitchInsn(int min, int max, Label dflt, Label[] labels)
            {
            }

            @Override
            public AnnotationVisitor visitParameterAnnotation(int parameter, String desc, boolean visible)
            {
                return new EmptyAnnotationVisitor(desc);
            }

            @Override
            public void visitMultiANewArrayInsn(String desc, int dims)
            {
            }

            @Override
            public void visitMethodInsn(int opcode, String owner, String name, String desc)
            {
            }

            @Override
            public void visitMaxs(int maxStack, int maxLocals)
            {
            }

            @Override
            public void visitLookupSwitchInsn(Label dflt, int[] keys, Label[] labels)
            {
            }

            @Override
            public void visitLocalVariable(String name, String desc, String signature, Label start, Label end, int index)
            {
            }

            @Override
            public void visitLineNumber(int line, Label start)
            {
            }

            @Override
            public void visitLdcInsn(Object cst)
            {
            }

            @Override
            public void visitLabel(Label label)
            {
            }

            @Override
            public void visitJumpInsn(int opcode, Label label)
            {
            }

            @Override
            public void visitIntInsn(int opcode, int operand)
            {
            }

            @Override
            public void visitInsn(int opcode)
            {
            }

            @Override
            public void visitIincInsn(int var, int increment)
            {
            }

            @Override
            public void visitFrame(int type, int nLocal, Object[] local, int nStack, Object[] stack)
            {
            }

            @Override
            public void visitFieldInsn(int opcode, String owner, String name, String desc)
            {
            }

            @Override
            public void visitEnd()
            {
            }

            @Override
            public void visitCode()
            {
            }

            @Override
            public void visitAttribute(Attribute attr)
            {
            }

            @Override
            public AnnotationVisitor visitAnnotationDefault()
            {
                return new EmptyAnnotationVisitor("");
            }

            @Override
            public AnnotationVisitor visitAnnotation(String desc, boolean visible)
            {
                return new EmptyAnnotationVisitor(desc);
            }
        };
    }

    @Override
    public void visitEnd()
    {
        Collections.sort(_columnFields);
        _logger.trace("END");
    }

    private class EmptyAnnotationVisitor implements AnnotationVisitor
    {
        private String _anno;
        public EmptyAnnotationVisitor(String anno)
        {
            _anno = anno;
        }
        @Override
        public void visit(String name, Object value)
        {
            _logger.trace(_anno + " prop " + name + ":" + value);
        }

        @Override
        public void visitEnum(String name, String desc, String value)
        {
            _logger.trace(_anno + " enum " + name + ":" + value);
        }

        @Override
        public AnnotationVisitor visitAnnotation(String name, String desc)
        {
            _logger.trace(_anno + " anno" + name + ":" + desc);
            return new EmptyAnnotationVisitor(_anno + "." + name);
        }

        @Override
        public AnnotationVisitor visitArray(String name)
        {
            _logger.trace(_anno + " array " + name);
            return this;
        }

        @Override
        public void visitEnd()
        {
        }
    }


}
