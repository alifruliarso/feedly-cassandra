package com.feedly.cassandra.bean.enhance;

import java.util.BitSet;

//should never be directly implemented
public interface IEnhancedBean
{
    public BitSet getModifiedFields();
    public void setModifiedFields(BitSet b);
    
    public boolean getUnmappedFieldsModified();
    public void setUnmappedFieldsModified(boolean b);
}
