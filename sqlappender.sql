using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using log4net.Appender;
using log4net.Core;
using log4net.Layout;

namespace PicNet.Util
{
  /**
   Create Log table with the following SQL:
    
   CREATE TABLE [Log] (
     [LogID] BIGINT IDENTITY (1, 1) NOT NULL,
     [DateCreated] DATETIME NOT NULL,
     [Thread] NVARCHAR (255) NOT NULL,
     [Level] NVARCHAR (50) NOT NULL,
     [Logger] NVARCHAR (255) NOT NULL,
     [Message] NVARCHAR (4000) NOT NULL,
     [Exception] NVARCHAR (2000) NULL,
     primary key (LogID)
   );    
   */  
  public class DatabaseLogAppender : BufferingAppenderSkeleton
  {        
    
    private static readonly RawTimeStampLayout TIME_STAMP_LAYOUT = new RawTimeStampLayout();
    private static readonly ExceptionLayout EXCEPTION_LAYOUT = new ExceptionLayout();
    private static readonly PatternLayout THREAD_LAYOUT = new PatternLayout("%thread");
    private static readonly PatternLayout LEVEL_LAYOUT = new PatternLayout("%level");
    private static readonly PatternLayout LOGGER_LAYOUT = new PatternLayout("%logger");
    private static readonly PatternLayout MESSAGE_LAYOUT = new PatternLayout("%message");    

    private const int MAX_QUEUE_SIZE = 100;
    private readonly Queue<string> logs = new Queue<string>();

    private readonly string connectionString;
    private readonly string tableName;

    public DatabaseLogAppender(string connectionString, string tableName)
    {
      this.connectionString = connectionString;
      this.tableName = tableName;
    }

    public string[] GetLastXLogs()
    {
      Flush();
      return logs.ToArray();
    }

    protected override void SendBuffer(LoggingEvent[] events) {
      if (String.IsNullOrEmpty(connectionString)) { return; }
      
      using (DataTable dt = GetDataTableFromEntries(events))
      {
        BatchInsertSQLServerData(dt);
      }
    }

    private void BatchInsertSQLServerData(DataTable dt) {      
      using (var connection = new SqlConnection(connectionString)) 
      {
        using (var batcher = new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction, null))
        {
          batcher.DestinationTableName = tableName;
          connection.Open();
          batcher.WriteToServer(dt);
        }
      }
    }

    private DataTable GetDataTableFromEntries(IEnumerable<LoggingEvent> logEntries) {
      DataTable dt = CreateDataTable();
      foreach (DataRow dr in logEntries.Select(le => PopulateAndQueueDataRow(le, dt.NewRow()))) {
        dt.Rows.Add(dr);
      }
      return dt;
    }    

    private DataTable CreateDataTable()
    {
      DataTable dt = ExecuteDataSet("SELECT TOP(0) * FROM [" + tableName + "]"). Tables[0];
      dt.TableName = tableName;
      return dt;
    }

    private DataSet ExecuteDataSet(string sql)
    {
      IDbCommand cmd = GetCommand(sql);
      using (SqlDataAdapter da = new SqlDataAdapter((SqlCommand) cmd))
      {
        DataSet ds = new DataSet();
        da.Fill(ds);
        return ds;
      }      
    }

    private IDbCommand GetCommand(string sql)
    {
      using (SqlConnection connection = new SqlConnection(connectionString))
      {
        IDbCommand c = connection.CreateCommand();        
        c.CommandText = sql;
        c.CommandType = CommandType.Text;        
        return c;
      }
    }

    private object PopulateAndQueueDataRow(LoggingEvent e, DataRow row)
    {
      DateTime dateCreated = (DateTime) TIME_STAMP_LAYOUT.Format(e);
      string dateCreatedStr = dateCreated.ToString("dd/MMM/yyyy HH:mm:ss");
      string thread = GetPatternLayoutData(THREAD_LAYOUT, 255, e);
      string level = GetPatternLayoutData(LEVEL_LAYOUT, 50, e);
      string logger = GetPatternLayoutData(LOGGER_LAYOUT, 255, e);
      string message = GetPatternLayoutData(MESSAGE_LAYOUT, 4000, e);
      string exception = GetExceptionLayoutData(2000, e);
      AddLogToQueue(dateCreatedStr, thread, level, logger, message, exception);

      row["DateCreated"] = dateCreated;
      row["Thread"] = thread;
      row["Level"] = level;
      row["Logger"] = logger;
      row["Message"] = message;
      row["Exception"] = exception;

      return row;
    }

    private void AddLogToQueue(string dateCreatedStr, string thread, string level, string logger, string message, string exception)
    {
      while (logs.Count >= MAX_QUEUE_SIZE) { logs.Dequeue(); }
      logs.Enqueue(String.Format("{0},{1}\t{2}\t{3} - {4} {5}", dateCreatedStr, thread, level, logger, message, exception));
    }


    private static string GetLayoutData(int maxsize, LoggingEvent loggevent, GetLayoutDataDelegate getLayoutDataDelegate) {      
      if (getLayoutDataDelegate == null) {
        throw new ApplicationException("getLayoutDataDelegate cannot be null");
      }
      using (TextWriter writer = new StringWriter())
      {
        getLayoutDataDelegate(writer, loggevent);
        string patternLayoutData = writer.ToString();
        return patternLayoutData.Length > maxsize ? patternLayoutData.Substring(0, maxsize) : patternLayoutData;
      }      
    }

    private static string GetPatternLayoutData(ILayout layout, int maxsize, LoggingEvent loggevent)
    {
      return GetLayoutData(maxsize, loggevent, (textWriter, loggevent1) => layout.Format(textWriter, loggevent));
    }

    private static string GetExceptionLayoutData(int maxsize, LoggingEvent loggevent)
    {
      return GetLayoutData(maxsize, loggevent, (textWriter, loggevent1) => EXCEPTION_LAYOUT.Format(textWriter, loggevent));
    }

    private delegate void GetLayoutDataDelegate(TextWriter writer, LoggingEvent loggevent);
  }
}