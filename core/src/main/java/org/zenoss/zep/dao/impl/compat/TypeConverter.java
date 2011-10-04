package org.zenoss.zep.dao.impl.compat;

/**
 * Generic interface used to convert parameters to/from database representations.
 */
public interface TypeConverter<T> {
    /**
     * Converts the original type to the database type.
     * 
     * @param originalType Original type.
     * @return Underlying type expected by the database.
     */
    public Object toDatabaseType(T originalType);

    /**
     * Converts the type returned by the database to the appropriate application type.
     *
     * @param databaseType Database type.
     * @return Application type.
     */
    public T fromDatabaseType(Object databaseType);
}
