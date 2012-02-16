package com.feedly.cassandra.entity.enhance;

import java.util.BitSet;

//should never be directly implemented
public interface IEnhancedEntity
{
    public BitSet getModifiedFields();
    public void setModifiedFields(BitSet b);
    
    public boolean getUnmappedFieldsModified();
    public void setUnmappedFieldsModified(boolean b);
}
