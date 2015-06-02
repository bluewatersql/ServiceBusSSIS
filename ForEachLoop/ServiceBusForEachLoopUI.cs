using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Runtime.Design;
using System;
using System.Collections;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Text;
using System.Windows.Forms;
using BluewaterSQL.DTS.ServiceBus.Enumerators;

namespace BluewaterSQL.DTS.ServiceBus.Enumerators.UI
{
    public sealed class ServiceBusForEachLoopUI : ForEachEnumeratorUI
    {
        #region Members
        private ServiceBusForEachLoop ssisObj;
        private IDtsConnectionService connections;

        private static readonly string NEW_CONNECTION = "New Connection...";
        private Label lblConnection;
        private ComboBox cbConnection;
        private Label lblTimeout;
        private TextBox txtTimeout;
        private Label lblBatchSize;
        private Label lblMessageCap;
        private TextBox txtBatchSize;
        private TextBox txtMessageCap;
        private static readonly string CONNECTION_TYPE = "AZURESVCBUS";
        #endregion

        public ServiceBusForEachLoopUI()
        {
            this.InitializeComponent();
            this.SuspendLayout();
            this.Anchor = AnchorStyles.Top | AnchorStyles.Bottom | AnchorStyles.Left | AnchorStyles.Right;
            this.ResumeLayout(false);
        }

        public override void Initialize(ForEachEnumeratorHost FEEHost, IServiceProvider serviceProvider, Connections connections, Variables variables)
        {
            this.connections = serviceProvider.GetService(typeof(IDtsConnectionService)) as IDtsConnectionService;

            try
            {
                this.ssisObj = FEEHost.InnerObject as ServiceBusForEachLoop;
            }
            catch (Exception)
            {
                this.ssisObj = null;
            }

            if (ssisObj == null)
                return;

            txtTimeout.Text = this.ssisObj.ServerWaitTimeOut.ToString();
            txtBatchSize.Text = this.ssisObj.BatchSize.ToString();
            txtMessageCap.Text = this.ssisObj.MessageCap.ToString();

            string conn = this.ssisObj.Connection;

            if (this.connections != null)
                this.cbConnection.Items.Add(NEW_CONNECTION);

            if (connections.Count <= 0)
                return;

            for (int i = 0; i < connections.Count; i++)
            {
                var connMgr = connections[i];

                if (IsValidConnection(connMgr))
                {
                    this.cbConnection.Items.Add(connMgr.Name);
                }
            }

            if (this.cbConnection.Items.Count <= 0 || string.IsNullOrEmpty(conn))
                return;

            this.cbConnection.SelectedItem = conn;

        }

        private void InitializeComponent()
        {
            this.lblConnection = new System.Windows.Forms.Label();
            this.cbConnection = new System.Windows.Forms.ComboBox();
            this.lblTimeout = new System.Windows.Forms.Label();
            this.txtTimeout = new System.Windows.Forms.TextBox();
            this.lblBatchSize = new System.Windows.Forms.Label();
            this.lblMessageCap = new System.Windows.Forms.Label();
            this.txtBatchSize = new System.Windows.Forms.TextBox();
            this.txtMessageCap = new System.Windows.Forms.TextBox();
            this.SuspendLayout();
            // 
            // lblConnection
            // 
            this.lblConnection.Location = new System.Drawing.Point(2, 0);
            this.lblConnection.Name = "lblConnection";
            this.lblConnection.Size = new System.Drawing.Size(100, 20);
            this.lblConnection.TabIndex = 3;
            this.lblConnection.Text = "Connection";
            // 
            // cbConnection
            // 
            this.cbConnection.Location = new System.Drawing.Point(2, 22);
            this.cbConnection.Name = "cbConnection";
            this.cbConnection.Size = new System.Drawing.Size(338, 24);
            this.cbConnection.TabIndex = 4;
            // 
            // lblTimeout
            // 
            this.lblTimeout.AutoSize = true;
            this.lblTimeout.Location = new System.Drawing.Point(2, 58);
            this.lblTimeout.Name = "lblTimeout";
            this.lblTimeout.Size = new System.Drawing.Size(261, 17);
            this.lblTimeout.TabIndex = 5;
            this.lblTimeout.Text = "Server Request Wait Timeout (seconds)";
            // 
            // txtTimeout
            // 
            this.txtTimeout.Location = new System.Drawing.Point(2, 78);
            this.txtTimeout.Name = "txtTimeout";
            this.txtTimeout.Size = new System.Drawing.Size(74, 22);
            this.txtTimeout.TabIndex = 6;
            this.txtTimeout.KeyPress += txtTimeout_KeyPress;
            // 
            // lblBatchSize
            // 
            this.lblBatchSize.AutoSize = true;
            this.lblBatchSize.Location = new System.Drawing.Point(2, 112);
            this.lblBatchSize.Name = "lblBatchSize";
            this.lblBatchSize.Size = new System.Drawing.Size(75, 17);
            this.lblBatchSize.TabIndex = 7;
            this.lblBatchSize.Text = "Batch Size";
            // 
            // lblMessageCap
            // 
            this.lblMessageCap.AutoSize = true;
            this.lblMessageCap.Location = new System.Drawing.Point(-1, 166);
            this.lblMessageCap.Name = "lblMessageCap";
            this.lblMessageCap.Size = new System.Drawing.Size(94, 17);
            this.lblMessageCap.TabIndex = 8;
            this.lblMessageCap.Text = "Message Cap";
            // 
            // txtBatchSize
            // 
            this.txtBatchSize.Location = new System.Drawing.Point(2, 132);
            this.txtBatchSize.Name = "txtBatchSize";
            this.txtBatchSize.Size = new System.Drawing.Size(74, 22);
            this.txtBatchSize.TabIndex = 9;
            this.txtBatchSize.KeyPress += txtBatchSize_KeyPress;
            // 
            // txtMessageCap
            // 
            this.txtMessageCap.Location = new System.Drawing.Point(2, 186);
            this.txtMessageCap.Name = "txtMessageCap";
            this.txtMessageCap.Size = new System.Drawing.Size(74, 22);
            this.txtMessageCap.TabIndex = 10;
            this.txtMessageCap.KeyPress += txtMessageCap_KeyPress;
            // 
            // ServiceBusForEachLoopUI
            // 
            this.Controls.Add(this.txtMessageCap);
            this.Controls.Add(this.txtBatchSize);
            this.Controls.Add(this.lblMessageCap);
            this.Controls.Add(this.lblBatchSize);
            this.Controls.Add(this.lblConnection);
            this.Controls.Add(this.cbConnection);
            this.Controls.Add(this.lblTimeout);
            this.Controls.Add(this.txtTimeout);
            this.Name = "ServiceBusForEachLoopUI";
            this.Size = new System.Drawing.Size(348, 222);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        private void txtMessageCap_KeyPress(object sender, KeyPressEventArgs e)
        {
            if (!char.IsControl(e.KeyChar) && !char.IsDigit(e.KeyChar) && e.KeyChar != '-')
            {
                e.Handled = true;
            }
        }

        private void txtBatchSize_KeyPress(object sender, KeyPressEventArgs e)
        {
            if (!char.IsControl(e.KeyChar) && !char.IsDigit(e.KeyChar) && e.KeyChar != '-' )
            {
                e.Handled = true;
            }
        }

        private void txtTimeout_KeyPress(object sender, KeyPressEventArgs e)
        {
            if (!char.IsControl(e.KeyChar) && !char.IsDigit(e.KeyChar))
            {
                e.Handled = true;
            }
        }

        private void cbConnection_SelectionChangeCommitted(object sender, EventArgs e)
        {
            string conn = (string)this.cbConnection.SelectedItem;

            if (conn.Equals(NEW_CONNECTION))
            {
                ArrayList connection = this.connections.CreateConnection(CONNECTION_TYPE);

                if (connection != null && connection.Count > 0)
                {
                    var connectionManager = (Microsoft.SqlServer.Dts.Runtime.ConnectionManager)connection[0];

                    if (this.IsValidConnection(connectionManager))
                    {
                        this.cbConnection.Items.Add(connectionManager.Name);
                        this.cbConnection.SelectedItem = connectionManager.Name;
                    }
                }
                else
                {
                    this.cbConnection.SelectedIndex = -1;
                }
            }
        }

        public override void SaveSettings()
        {
            if (ssisObj == null)
                return;

            ssisObj.Connection = (string)cbConnection.SelectedItem;
            ssisObj.ServerWaitTimeOut = Convert.ToInt32(txtTimeout.Text);
            ssisObj.BatchSize = Convert.ToInt32(txtBatchSize.Text);
            ssisObj.MessageCap = Convert.ToInt32(txtMessageCap.Text);
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
        }

        #region Helper Methods
        private bool IsValidConnection(Microsoft.SqlServer.Dts.Runtime.ConnectionManager connMgr)
        {
            string creationName = connMgr.CreationName;

            if (creationName == CONNECTION_TYPE)
                return true;
            
            return false;
        }
        #endregion
    }
}
