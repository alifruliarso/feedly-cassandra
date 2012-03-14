package com.feedly.cassandra.entity.enhance;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.types.FileSet;
import org.objectweb.asm.ClassReader;

import com.feedly.cassandra.FeedlyLogFormatter;
import com.feedly.cassandra.dao.CassandraDaoBase;
import com.feedly.cassandra.entity.EntityUtils;

import feedly.net.sf.cglib.transform.AbstractTransformTask;
import feedly.net.sf.cglib.transform.ClassTransformer;
import feedly.net.sf.cglib.transform.ClassTransformerChain;
import feedly.net.sf.cglib.transform.MethodFilter;
import feedly.net.sf.cglib.transform.MethodFilterTransformer;
import feedly.net.sf.cglib.transform.impl.AddInitTransformer;

/**
 * This ant task should be run post compilation on Entity classes. It will perform the necessary byte code manipulation to track modified
 * fields. This is used by {@link CassandraDaoBase} to only write modified fields to the database.
 * 
 * @author kireet
 */
public class ColumnFamilyTransformTask extends AbstractTransformTask
{
    private List<File> _files = new ArrayList<File>();
    private Map<String, BeanInfoVisitor> _beanInfo = new HashMap<String, BeanInfoVisitor>();
    private boolean verbose;

    @Override
    public void setVerbose(boolean verbose)
    {
        super.setVerbose(verbose);
        this.verbose = verbose;
    }

    @Override
    protected Collection<?> getFiles()
    {
        return _files;
    }
    
//    private static ClassReader getClassReader(File file) throws Exception {
//        InputStream in = new BufferedInputStream(new FileInputStream(file));
//        try {
//            ClassReader r = new ClassReader(in);
//            return r;
//        } finally {
//            in.close();
//        }
//
//    }
//
//    @Override
//    protected void processFile(File file) throws Exception {
//
//
//        ClassReader reader = getClassReader(file);
//        String name[] = ClassNameReader.getClassInfo(reader);
//        ClassWriter w = new DebuggingClassWriter(ClassWriter.COMPUTE_MAXS);
//        ClassTransformer t = getClassTransformer(name);
//        if (t != null) {
//
//            if (verbose) {
//                log("processing " + file.toURL());
//            }
//            new TransformingClassGenerator(new ClassReaderGenerator(
//                    getClassReader(file), attributes(), getFlags()), t)
//                    .generateClass(w);
//            File out = new File(file.getParent(), "Output.class");
//            if(out.exists())
//                out.delete();
//            FileOutputStream fos = new FileOutputStream(out);
//            try {
//                fos.write(w.toByteArray());
//            } finally {
//                fos.close();
//            }
//
//        }
//    }
    
    @Override
    protected void beforeExecute() throws BuildException
    {
        int alreadyImplementedCount = 0;
        int toEnhanceCount = 0;
        int totalCnt = 0;
        for(Object file : super.getFiles())
        {
            totalCnt++;
            
            File f = (File) file;
            try
            {
                BeanInfoVisitor visitor = new BeanInfoVisitor();
                ClassReader reader = new ClassReader(new FileInputStream(f));
                reader.accept(visitor, 0);

                if(visitor.alreadyImplementsEnhancer())
                    alreadyImplementedCount++;
                else if(visitor.hasColFamilyAnnotation())
                {
                    _files.add(f);
                    _beanInfo.put(visitor.getBeanClassName(), visitor);
                    toEnhanceCount++;
                }
            }
            catch(Exception fnfe)
            {
                throw new BuildException("error loading class file " + f.getAbsolutePath(), fnfe);
            }
        }

        log(String.format("Examined %d files, %d already enhanced, %d will be enhanced", totalCnt, alreadyImplementedCount, toEnhanceCount));
        if(verbose)
        {
            for(Entry<String, BeanInfoVisitor> e : _beanInfo.entrySet())
                log(" -> " + e.getValue());
        }

    }

    @Override
    protected ClassTransformer getClassTransformer(String[] classInfo)
    {
        try
        {
            AddInitTransformer init = new AddInitTransformer(EntityUtils.class.getMethod("initBitSet", Object.class));
            AddInterfaceTransformer iface = new AddInterfaceTransformer(IEnhancedEntity.class);
            // AddPropertyTransformer prop = new AddPropertyTransformer(new
            // String[]{"modifiedFields"}, new
            // Type[]{Type.getType(BitSet.class)});

            final BeanInfoVisitor info = _beanInfo.get(classInfo[0]);
            ColumnPropertyEnhancer enh = new ColumnPropertyEnhancer(info);
            MethodFilterTransformer mf = new MethodFilterTransformer(new MethodFilter()
            {

                @Override
                public boolean accept(int access, String name, String desc, String signature, String[] exceptions)
                {

                    String field = Character.toLowerCase(name.charAt(3)) + name.substring(4);

                    boolean isSetter = name.startsWith("set") && name.length() > 3;
                    boolean isGetter = name.startsWith("get") && name.length() > 3;

                    boolean rv = field.equals(info.getUnmappedField()) || info.getColumnFields().indexOf(field) >= 0 && (isSetter || (info.getColumnCollectionFields().contains(field) && isGetter));
                    
                    return rv;
                }

            }, enh);
            return new ClassTransformerChain(new feedly.net.sf.cglib.transform.ClassTransformer[]
            { init, iface, mf });
        }
        catch(Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    public static void main(String[] args) throws Throwable
    {
        Logger.getLogger("com.feedly").setLevel(Level.FINEST);

        for(Handler h : Logger.getLogger("").getHandlers())
        {
            h.setLevel(Level.ALL);
            h.setFormatter(new FeedlyLogFormatter());
        }

        Project p = new Project();
        p.setBaseDir(new File("."));

        ColumnFamilyTransformTask cftt = new ColumnFamilyTransformTask();
        cftt.setProject(p);
        FileSet fileSet = new FileSet();
//        fileSet.setDir(new File("target/test-classes/com/feedly/cassandra/bean/enhance"));
        fileSet.setFile(new File("target/test-classes/com/feedly/cassandra/bean/enhance/MapBean.class"));
        cftt.addFileset(fileSet);
        cftt.execute();
    }
}
