using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading;

namespace SqlBulkCopyDemo
{
    /// <summary>
    /// 数据导入类
    /// </summary>
    class BulkImportData
    {
        SynchronizationContext m_context;
        SendOrPostCallback m_callback;
        /// <summary>
        /// 待导入数据的目标数据库；配置文件中的连接字符串
        /// </summary>
        private string Connectstring {
            get {
                string cstr = "";
                if (ConfigurationManager.ConnectionStrings["conn"] != null) {
                    cstr = ConfigurationManager.ConnectionStrings["conn"].ConnectionString;
                }
                return cstr;
            }
        }
        public BulkImportData(SynchronizationContext context,SendOrPostCallback callback) {
            m_context = context;
            m_callback = callback;
        }
        /// <summary>
        /// 导入数据
        /// </summary>
        /// <param name="table">源数据</param>
        public void BeginImportData(DataTable table) {
            Thread thread = new Thread(new ParameterizedThreadStart(BeginImport));
            thread.Start(table);
        }
        /// <summary>
        /// 执行数据导入
        /// </summary>
        /// <param name="data"></param>
        private void BeginImport(object data) {
            DataTable dt = data as DataTable;
            if (dt == null || string.IsNullOrWhiteSpace(Connectstring)) {
                return;
            }
            m_context.Post(m_callback, null);
            try {
                using (SqlBulkCopy sbc = new SqlBulkCopy(Connectstring, SqlBulkCopyOptions.UseInternalTransaction)) {
                    Stopwatch sw = new Stopwatch();
                    sw.Start();
                    sbc.BatchSize = dt.Rows.Count;
                    sbc.BulkCopyTimeout = 120;
                    sbc.DestinationTableName = dt.TableName;
                    sbc.NotifyAfter = 3;
                    sbc.SqlRowsCopied += ImportSqlRowsCopied;
                    foreach (DataColumn col in dt.Columns) {
                        sbc.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                    }
                    sw.Stop();
                    m_context.Post(m_callback, string.Format("准备数据用时{0}毫秒",sw.ElapsedMilliseconds));
                    sw.Restart();
                    sbc.WriteToServer(dt);
                    m_context.Post(m_callback, string.Format("{0}条数据导入完成,共用时{1}毫秒",dt.Rows.Count, sw.ElapsedMilliseconds));
                    dt.Dispose();
                }
            } catch (Exception ex) {
                Trace.WriteLine(ex.Message);
                m_context.Post(m_callback, ex.Message);
            }
        }
        /// <summary>
        /// 导入指定行后执行
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void ImportSqlRowsCopied(object sender, SqlRowsCopiedEventArgs e) {
            m_context.Post(m_callback, 3);
        }
    }
}
