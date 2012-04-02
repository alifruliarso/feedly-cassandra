package com.feedly.cassandra.entity.enhance;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.FileSet;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.util.CheckClassAdapter;

public class EntityTransformerTask extends Task
{
    private boolean _verbose;
    private List<FileSet> _filesets = new ArrayList<FileSet>();

    public boolean getVerbose()
    {
        return _verbose;
    }

    public void setVerbose(boolean verbose)
    {
        _verbose = verbose;
        log("verbose set to " + verbose);
    }

    public void addFileset(FileSet set)
    {
        _filesets.add(set);
    }
    
    @Override
    public void execute() throws BuildException
    {
        logv("processing ", _filesets.size(), " filesets.");
        int total = 0;
        int cnt = 0;
        
        for(FileSet fset : _filesets)
        {
            DirectoryScanner ds = fset.getDirectoryScanner(getProject());
            for(String fname : ds.getIncludedFiles())
            {
                if(fname.endsWith(".class"))
                {
                    File f = new File(ds.getBasedir(), fname);
                    if(f.exists() && f.canRead() && f.canWrite())
                    {
                        EntityTransformer t = new EntityTransformer(null);
                        FileInputStream fis = null;
                        FileOutputStream fos = null;
                        try
                        {
                            fis = new FileInputStream(f);
                            ClassReader cr = new ClassReader(fis);
                            ClassNode cn = new ClassNode();
                            cr.accept(cn, 0);

                            boolean modified = t.transform(cn);
                            total++;
                            if(modified)
                            {
                                ClassWriter cw = new ClassWriter(0);
                                CheckClassAdapter cca = new CheckClassAdapter(cw);
                                cn.accept(cca);

                                f.delete();
                                fos = new FileOutputStream(f);
                                fos.write(cw.toByteArray());
                                
                                logv("processed ", f.getAbsolutePath());
                                cnt++;
                            }
                            else
                                logv("skipped class ", f.getAbsolutePath());
                        }
                        catch(IOException ex)
                        {
                            throw new BuildException("error processing" + fname, ex);
                        }
                        finally
                        {
                            try
                            {
                                if(fis != null)
                                    fis.close();
                                if(fos != null)
                                    fos.close();
                            }
                            catch(IOException ex) {}
                        }
                    }
                }
            }
        }
        
        log(String.format("%s examined, %s enhanced", total, cnt));
    }
    
    private void logv(Object... parts)
    {
        if(_verbose)
        {
            StringBuilder b = new StringBuilder();
            for(Object p : parts)
                b.append(p);
            
            log(b.toString());
        }
    }
}
