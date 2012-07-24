/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao.impl.compat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Basic utilities for performing type conversion.
 */
public class TypeConverterUtils {
    /**
     * Performs a batch conversion of native types to database types using the specified TypeConverter.
     *
     * @param typeConverter TypeConverter to use to convert the types.
     * @param container Container containing the objects to convert to their database representations.
     * @param <T> The application type of objects.
     * @return A converted list of application types to the corresponding database type.
     */
    public static <T> List<Object> batchToDatabaseType(TypeConverter<T> typeConverter, Collection<T> container) {
        List<Object> converted = new ArrayList<Object>(container.size());
        for (T appType : container) {
            converted.add(typeConverter.toDatabaseType(appType));
        }
        return converted;
    }
}
