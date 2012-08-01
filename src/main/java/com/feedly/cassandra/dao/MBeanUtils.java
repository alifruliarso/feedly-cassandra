package com.feedly.cassandra.dao;

import java.net.URL;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

class MBeanUtils
{
    static final int DEFAULT_STATS_SIZE = 10000;
    
    static String getUniqueClassloaderIdentifier(Class<?> clazz) 
    {
        String contextPath = getContextPath(clazz);
        if (contextPath != null) 
            return contextPath;
        
        return "feedly-cassandra";
    }

    static String getContextPath(Class<?> clazz) 
    {
        ClassLoader loader = clazz.getClassLoader();
        if(loader == null)
            return null;
        URL url = loader.getResource("/");
        if (url != null) 
        {
            String[] elements = url.toString().split("/");
            for (int i = elements.length - 1; i > 0; --i) 
            {
                // URLs look like this: file:/.../ImageServer/WEB-INF/classes/
                // And we want that part that's just before WEB-INF
                if ("WEB-INF".equals(elements[i])) 
                    return elements[i - 1];
                
            }
        }
        return null;
    }
    
    static ObjectName mBeanName(Object monitored, String qualifier, String name) throws MalformedObjectNameException
    {
        return new ObjectName(String.format("feedly.cassandra.%s%s%s:context=%s,name=%s", 
                                            monitored.getClass().getSimpleName(), 
                                            qualifier != null ? "_" : "",
                                            qualifier != null ? qualifier : "",
                                            MBeanUtils.getUniqueClassloaderIdentifier(monitored.getClass()),
                                            name));
    }
}
