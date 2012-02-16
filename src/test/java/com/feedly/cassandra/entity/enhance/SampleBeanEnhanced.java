package com.feedly.cassandra.entity.enhance;

import java.util.BitSet;

import com.feedly.cassandra.anno.Column;

/**
 * unused, just here as a convenience to compare byte codes when developing cglib transforms
 * The javap -c option is your friend.
 * 
 * @author kireet
 */
public class SampleBeanEnhanced implements IEnhancedEntity
{
    private BitSet $cglib_prop_modifiedFields;
    
    private void initBitSet(Object o)
    {
        if(o instanceof IEnhancedEntity)
        {
            ((IEnhancedEntity) o).setModifiedFields(new BitSet());
        }
    }
    
    public SampleBeanEnhanced()
    {
        initBitSet(this);
    }
    
    @Column
    private String val1;
    
    private String val2;

    public String getVal1()
    {
        return val1;
    }

    public void setVal1(String field)
    {
        this.val1 = field;
        $cglib_prop_modifiedFields.set(0);
    }

    public String getVal2()
    {
        return val2;
    }

    public void setVal2(String val2)
    {
        this.val2 = val2;
    }

    
    @Override
    public BitSet getModifiedFields()
    {
        return $cglib_prop_modifiedFields;
    }

    @Override
    public void setModifiedFields(BitSet b)
    {
        $cglib_prop_modifiedFields = b;
    }

    @Override
    public boolean getUnmappedFieldsModified()
    {
        return false;
    }

    @Override
    public void setUnmappedFieldsModified(boolean b)
    {
    }
    

}
