package com.feedly.cassandra;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class FeedlyLogFormatter extends Formatter
{

    private final DateFormat format = new SimpleDateFormat("HH:mm:ss");
    private static final String lineSep = System.getProperty("line.separator");

    @Override
    public String format(LogRecord record)
    {
        String loggerName = record.getLoggerName();
        if(loggerName == null) {
            loggerName = "root";
        }
        
        String dateStr = null;        
        synchronized (format)
        {
            dateStr = format.format(new Date(record.getMillis()));
        }
        
        StringBuilder output = new StringBuilder()
            .append(record.getSourceClassName().replace("com.devhd.feedly.", ""))
            .append(".")
            .append(record.getSourceMethodName())
            .append("[")
            .append(record.getLevel()).append('|')
            .append(Thread.currentThread().getName()).append('|')
            .append(dateStr)
            .append("]: ")
            .append(record.getMessage()).append(' ')
            .append(lineSep);
        if(record.getThrown() != null)
        {
            StringWriter sw = new StringWriter();
            record.getThrown().printStackTrace(new PrintWriter(sw));
            
            output.append(sw.getBuffer());
        }
        
        return output.toString();               
    }

}
