package com.feedly.cassandra.entity.enhance;

import java.util.BitSet;

import com.feedly.cassandra.dao.CassandraDaoBase;

/**
 * This class is used by {@link CassandraDaoBase} to determine which fields have been updated and should be written or used for index finds.
 * It should never be implemented directly, instead classes should be enhanced post compilation.
 * 
 * @author kireet
 * @see ColumnFamilyTransformTask
 */
public interface IEnhancedEntity
{
    public BitSet getModifiedFields();
    public void setModifiedFields(BitSet b);
    
    public boolean getUnmappedFieldsModified();
    public void setUnmappedFieldsModified(boolean b);
}
