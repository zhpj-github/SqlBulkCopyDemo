using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace SqlBulkCopyDemo
{
    public partial class MainForm : Form
    {
        BulkImportData m_bulkImport;
        private int m_importCount = 0;
        public MainForm() {
            InitializeComponent();
        }

        private void buttonBegin_Click(object sender, EventArgs e) {
            progressBar.Value = 0;
            DataTable dt = GetTestData();
            progressBar.Maximum = dt.Rows.Count;
            m_bulkImport.BeginImportData(GetTestData());
        }

        private void MainForm_Load(object sender, EventArgs e) {
            m_bulkImport = new BulkImportData(SynchronizationContext.Current, OnReceiveData);
        }
        /// <summary>
        /// 回调函数，导入部分数据后主界面结果展示
        /// </summary>
        /// <param name="state"></param>
        private void OnReceiveData(object state) {
            if (state != null) {
                int count = 0;
                if (int.TryParse(state.ToString(), out count)) {
                    if (count > 0) {
                        m_importCount += count;
                    }
                    listBoxResult.Items.Add(string.Format("已导入{0}条", m_importCount));
                    progressBar.Value = m_importCount;
                } else {
                    string result = state.ToString();
                    listBoxResult.Items.Add(result);
                    if (result.Contains("完成")) {
                        string[] strs = result.Split('条');
                        progressBar.Value = int.Parse(strs[0]);
                    }
                }
            } else {
                m_importCount = 0;
                listBoxResult.Items.Clear();
                listBoxResult.Items.Add("开始导入数据...");
            }
        }
        private DataTable GetTestData() {
            DataTable dt = new DataTable("dbo.sl_testTable");
            dt.Columns.Add("ID");
            dt.Columns.Add("Name");
            for (int i = 0; i < 10; i++) {
                dt.Rows.Add(i.ToString(), i.ToString());
            }
            return dt;
        }

        private void buttonCancel_Click(object sender, EventArgs e) {
            Close();
        }
    }
}
