using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Microsoft.SqlServer.Dts.Design;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace BluewaterSQL.DTS.ServiceBus.ConnectionManager
{
    public class ServiceBusConnectionManagerForm: Form
    {
        public ServiceBusConnectionManagerForm()
        {
            this.InitializeComponent();
        }

        private IServiceProvider _serviceProvider;
        private Microsoft.SqlServer.Dts.Runtime.ConnectionManager _connection;
        private ServiceBusConnectionManager _connectionManager;
        private IErrorCollectionService _errorCollector;
        private Label lblIssuer;
        private Label lblSecret;
        private Label lblMode;
        private Label lblQueue;
        private Label lblReceiveMode;
        private Label lblTopic;
        private Label lblSubscription;
        private GroupBox grpQueue;
        private ComboBox cmbReceiveMode;
        private TextBox txtQueue;
        private GroupBox grpTopic;
        private TextBox txtSubscription;
        private TextBox txtTopic;
        private TextBox txtEndpoint;
        private TextBox txtIssuer;
        private TextBox txtSecret;
        private ComboBox cmbMode;
        private Button btnOK;
        private Button btnCancel;
        private CheckBox chkCreate;
        private GroupBox grpGeneral;
        private TextBox txtDescription;
        private TextBox txtName;
        private Label lblDescription;
        private Label lblName;
        private Button btnTestConnection;

        private Label lblEndpoint;

        private void InitializeComponent()
        {
            this.lblEndpoint = new System.Windows.Forms.Label();
            this.lblIssuer = new System.Windows.Forms.Label();
            this.lblSecret = new System.Windows.Forms.Label();
            this.lblMode = new System.Windows.Forms.Label();
            this.lblQueue = new System.Windows.Forms.Label();
            this.lblReceiveMode = new System.Windows.Forms.Label();
            this.lblTopic = new System.Windows.Forms.Label();
            this.lblSubscription = new System.Windows.Forms.Label();
            this.grpQueue = new System.Windows.Forms.GroupBox();
            this.cmbReceiveMode = new System.Windows.Forms.ComboBox();
            this.txtQueue = new System.Windows.Forms.TextBox();
            this.grpTopic = new System.Windows.Forms.GroupBox();
            this.txtSubscription = new System.Windows.Forms.TextBox();
            this.txtTopic = new System.Windows.Forms.TextBox();
            this.txtEndpoint = new System.Windows.Forms.TextBox();
            this.txtIssuer = new System.Windows.Forms.TextBox();
            this.txtSecret = new System.Windows.Forms.TextBox();
            this.cmbMode = new System.Windows.Forms.ComboBox();
            this.btnOK = new System.Windows.Forms.Button();
            this.btnCancel = new System.Windows.Forms.Button();
            this.chkCreate = new System.Windows.Forms.CheckBox();
            this.grpGeneral = new System.Windows.Forms.GroupBox();
            this.txtDescription = new System.Windows.Forms.TextBox();
            this.txtName = new System.Windows.Forms.TextBox();
            this.lblDescription = new System.Windows.Forms.Label();
            this.lblName = new System.Windows.Forms.Label();
            this.btnTestConnection = new System.Windows.Forms.Button();
            this.grpQueue.SuspendLayout();
            this.grpTopic.SuspendLayout();
            this.grpGeneral.SuspendLayout();
            this.SuspendLayout();
            // 
            // lblEndpoint
            // 
            this.lblEndpoint.AutoSize = true;
            this.lblEndpoint.Location = new System.Drawing.Point(13, 93);
            this.lblEndpoint.Name = "lblEndpoint";
            this.lblEndpoint.Size = new System.Drawing.Size(64, 17);
            this.lblEndpoint.TabIndex = 0;
            this.lblEndpoint.Text = "Endpoint";
            // 
            // lblIssuer
            // 
            this.lblIssuer.AutoSize = true;
            this.lblIssuer.Location = new System.Drawing.Point(13, 138);
            this.lblIssuer.Name = "lblIssuer";
            this.lblIssuer.Size = new System.Drawing.Size(172, 17);
            this.lblIssuer.TabIndex = 1;
            this.lblIssuer.Text = "Shared Access Key Name";
            // 
            // lblSecret
            // 
            this.lblSecret.AutoSize = true;
            this.lblSecret.Location = new System.Drawing.Point(13, 183);
            this.lblSecret.Name = "lblSecret";
            this.lblSecret.Size = new System.Drawing.Size(131, 17);
            this.lblSecret.TabIndex = 2;
            this.lblSecret.Text = "Shared Access Key";
            // 
            // lblMode
            // 
            this.lblMode.AutoSize = true;
            this.lblMode.Location = new System.Drawing.Point(13, 228);
            this.lblMode.Name = "lblMode";
            this.lblMode.Size = new System.Drawing.Size(43, 17);
            this.lblMode.TabIndex = 3;
            this.lblMode.Text = "Mode";
            // 
            // lblQueue
            // 
            this.lblQueue.AutoSize = true;
            this.lblQueue.Location = new System.Drawing.Point(14, 35);
            this.lblQueue.Name = "lblQueue";
            this.lblQueue.Size = new System.Drawing.Size(51, 17);
            this.lblQueue.TabIndex = 4;
            this.lblQueue.Text = "Queue";
            // 
            // lblReceiveMode
            // 
            this.lblReceiveMode.AutoSize = true;
            this.lblReceiveMode.Location = new System.Drawing.Point(14, 80);
            this.lblReceiveMode.Name = "lblReceiveMode";
            this.lblReceiveMode.Size = new System.Drawing.Size(98, 17);
            this.lblReceiveMode.TabIndex = 5;
            this.lblReceiveMode.Text = "Receive Mode";
            // 
            // lblTopic
            // 
            this.lblTopic.AutoSize = true;
            this.lblTopic.Location = new System.Drawing.Point(14, 35);
            this.lblTopic.Name = "lblTopic";
            this.lblTopic.Size = new System.Drawing.Size(43, 17);
            this.lblTopic.TabIndex = 6;
            this.lblTopic.Text = "Topic";
            // 
            // lblSubscription
            // 
            this.lblSubscription.AutoSize = true;
            this.lblSubscription.Location = new System.Drawing.Point(14, 80);
            this.lblSubscription.Name = "lblSubscription";
            this.lblSubscription.Size = new System.Drawing.Size(86, 17);
            this.lblSubscription.TabIndex = 7;
            this.lblSubscription.Text = "Subscription";
            // 
            // grpQueue
            // 
            this.grpQueue.Controls.Add(this.cmbReceiveMode);
            this.grpQueue.Controls.Add(this.txtQueue);
            this.grpQueue.Controls.Add(this.lblQueue);
            this.grpQueue.Controls.Add(this.lblReceiveMode);
            this.grpQueue.Enabled = false;
            this.grpQueue.Location = new System.Drawing.Point(357, 93);
            this.grpQueue.Name = "grpQueue";
            this.grpQueue.Size = new System.Drawing.Size(333, 132);
            this.grpQueue.TabIndex = 8;
            this.grpQueue.TabStop = false;
            // 
            // cmbReceiveMode
            // 
            this.cmbReceiveMode.FormattingEnabled = true;
            this.cmbReceiveMode.Location = new System.Drawing.Point(9, 100);
            this.cmbReceiveMode.Name = "cmbReceiveMode";
            this.cmbReceiveMode.Size = new System.Drawing.Size(149, 24);
            this.cmbReceiveMode.TabIndex = 15;
            // 
            // txtQueue
            // 
            this.txtQueue.Location = new System.Drawing.Point(9, 55);
            this.txtQueue.Name = "txtQueue";
            this.txtQueue.Size = new System.Drawing.Size(313, 22);
            this.txtQueue.TabIndex = 14;
            // 
            // grpTopic
            // 
            this.grpTopic.Controls.Add(this.txtSubscription);
            this.grpTopic.Controls.Add(this.txtTopic);
            this.grpTopic.Controls.Add(this.lblSubscription);
            this.grpTopic.Controls.Add(this.lblTopic);
            this.grpTopic.Enabled = false;
            this.grpTopic.Location = new System.Drawing.Point(357, 231);
            this.grpTopic.Name = "grpTopic";
            this.grpTopic.Size = new System.Drawing.Size(333, 132);
            this.grpTopic.TabIndex = 9;
            this.grpTopic.TabStop = false;
            // 
            // txtSubscription
            // 
            this.txtSubscription.Location = new System.Drawing.Point(9, 100);
            this.txtSubscription.Name = "txtSubscription";
            this.txtSubscription.Size = new System.Drawing.Size(313, 22);
            this.txtSubscription.TabIndex = 12;
            // 
            // txtTopic
            // 
            this.txtTopic.Location = new System.Drawing.Point(9, 55);
            this.txtTopic.Name = "txtTopic";
            this.txtTopic.Size = new System.Drawing.Size(313, 22);
            this.txtTopic.TabIndex = 11;
            // 
            // txtEndpoint
            // 
            this.txtEndpoint.Location = new System.Drawing.Point(12, 113);
            this.txtEndpoint.Name = "txtEndpoint";
            this.txtEndpoint.Size = new System.Drawing.Size(313, 22);
            this.txtEndpoint.TabIndex = 10;
            // 
            // txtIssuer
            // 
            this.txtIssuer.Location = new System.Drawing.Point(12, 158);
            this.txtIssuer.Name = "txtIssuer";
            this.txtIssuer.Size = new System.Drawing.Size(313, 22);
            this.txtIssuer.TabIndex = 11;
            // 
            // txtSecret
            // 
            this.txtSecret.Location = new System.Drawing.Point(12, 203);
            this.txtSecret.Name = "txtSecret";
            this.txtSecret.Size = new System.Drawing.Size(313, 22);
            this.txtSecret.TabIndex = 12;
            // 
            // cmbMode
            // 
            this.cmbMode.FormattingEnabled = true;
            this.cmbMode.Location = new System.Drawing.Point(12, 249);
            this.cmbMode.Name = "cmbMode";
            this.cmbMode.Size = new System.Drawing.Size(121, 24);
            this.cmbMode.TabIndex = 13;
            // 
            // btnOK
            // 
            this.btnOK.Location = new System.Drawing.Point(615, 369);
            this.btnOK.Name = "btnOK";
            this.btnOK.Size = new System.Drawing.Size(75, 32);
            this.btnOK.TabIndex = 14;
            this.btnOK.Text = "OK";
            this.btnOK.UseVisualStyleBackColor = true;
            this.btnOK.Click += new System.EventHandler(this.btnOK_Click);
            // 
            // btnCancel
            // 
            this.btnCancel.Location = new System.Drawing.Point(534, 369);
            this.btnCancel.Name = "btnCancel";
            this.btnCancel.Size = new System.Drawing.Size(75, 32);
            this.btnCancel.TabIndex = 15;
            this.btnCancel.Text = "Cancel";
            this.btnCancel.UseVisualStyleBackColor = true;
            this.btnCancel.Click += new System.EventHandler(this.btnCancel_Click);
            // 
            // chkCreate
            // 
            this.chkCreate.AutoSize = true;
            this.chkCreate.Checked = true;
            this.chkCreate.CheckState = System.Windows.Forms.CheckState.Checked;
            this.chkCreate.Location = new System.Drawing.Point(16, 314);
            this.chkCreate.Name = "chkCreate";
            this.chkCreate.Size = new System.Drawing.Size(179, 21);
            this.chkCreate.TabIndex = 16;
            this.chkCreate.Text = "Create When Not Exists";
            this.chkCreate.UseVisualStyleBackColor = true;
            // 
            // grpGeneral
            // 
            this.grpGeneral.Controls.Add(this.txtDescription);
            this.grpGeneral.Controls.Add(this.txtName);
            this.grpGeneral.Controls.Add(this.lblDescription);
            this.grpGeneral.Controls.Add(this.lblName);
            this.grpGeneral.Location = new System.Drawing.Point(12, 5);
            this.grpGeneral.Name = "grpGeneral";
            this.grpGeneral.Size = new System.Drawing.Size(678, 82);
            this.grpGeneral.TabIndex = 17;
            this.grpGeneral.TabStop = false;
            // 
            // txtDescription
            // 
            this.txtDescription.Location = new System.Drawing.Point(354, 39);
            this.txtDescription.Name = "txtDescription";
            this.txtDescription.Size = new System.Drawing.Size(313, 22);
            this.txtDescription.TabIndex = 3;
            // 
            // txtName
            // 
            this.txtName.Location = new System.Drawing.Point(6, 38);
            this.txtName.Name = "txtName";
            this.txtName.Size = new System.Drawing.Size(307, 22);
            this.txtName.TabIndex = 2;
            // 
            // lblDescription
            // 
            this.lblDescription.AutoSize = true;
            this.lblDescription.Location = new System.Drawing.Point(351, 18);
            this.lblDescription.Name = "lblDescription";
            this.lblDescription.Size = new System.Drawing.Size(79, 17);
            this.lblDescription.TabIndex = 1;
            this.lblDescription.Text = "Description";
            // 
            // lblName
            // 
            this.lblName.AutoSize = true;
            this.lblName.Location = new System.Drawing.Point(6, 18);
            this.lblName.Name = "lblName";
            this.lblName.Size = new System.Drawing.Size(45, 17);
            this.lblName.TabIndex = 0;
            this.lblName.Text = "Name";
            // 
            // btnTestConnection
            // 
            this.btnTestConnection.Location = new System.Drawing.Point(12, 369);
            this.btnTestConnection.Name = "btnTestConnection";
            this.btnTestConnection.Size = new System.Drawing.Size(121, 32);
            this.btnTestConnection.TabIndex = 18;
            this.btnTestConnection.Text = "Test Connection";
            this.btnTestConnection.UseVisualStyleBackColor = true;
            this.btnTestConnection.Click += new System.EventHandler(this.btnTestConnection_Click);
            // 
            // ServiceBusConnectionManagerForm
            // 
            this.ClientSize = new System.Drawing.Size(706, 411);
            this.Controls.Add(this.btnTestConnection);
            this.Controls.Add(this.grpGeneral);
            this.Controls.Add(this.chkCreate);
            this.Controls.Add(this.btnCancel);
            this.Controls.Add(this.btnOK);
            this.Controls.Add(this.cmbMode);
            this.Controls.Add(this.txtSecret);
            this.Controls.Add(this.txtIssuer);
            this.Controls.Add(this.txtEndpoint);
            this.Controls.Add(this.grpTopic);
            this.Controls.Add(this.grpQueue);
            this.Controls.Add(this.lblMode);
            this.Controls.Add(this.lblSecret);
            this.Controls.Add(this.lblIssuer);
            this.Controls.Add(this.lblEndpoint);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedSingle;
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "ServiceBusConnectionManagerForm";
            this.ShowInTaskbar = false;
            this.SizeGripStyle = System.Windows.Forms.SizeGripStyle.Hide;
            this.Text = "Service Bus Connection Manager";
            this.Load += new System.EventHandler(this.ServiceBusConnectionManagerForm_Load);
            this.grpQueue.ResumeLayout(false);
            this.grpQueue.PerformLayout();
            this.grpTopic.ResumeLayout(false);
            this.grpTopic.PerformLayout();
            this.grpGeneral.ResumeLayout(false);
            this.grpGeneral.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        private void cmbMode_SelectionChangeCommitted(object sender, EventArgs e)
        {
            Modes mode = (Modes)Enum.Parse(typeof(Modes), cmbMode.SelectedItem.ToString());

            grpQueue.Enabled = (mode == Modes.QUEUE);
            grpTopic.Enabled = (mode != Modes.QUEUE);
            txtSubscription.Enabled = (mode == Modes.SUBSCRIPTION);
        }

        private void ServiceBusConnectionManagerForm_Load(object sender, EventArgs e)
        {
            Array modes = Enum.GetNames(typeof(Modes));

            foreach (var m in modes)
                cmbMode.Items.Add(m.ToString());
            
            Array receiveModes = Enum.GetNames(typeof(ReceiveMode));

            foreach (var r in receiveModes)
                cmbReceiveMode.Items.Add(r.ToString());

            if (_connection != null && _connectionManager != null)
            {
                txtName.Text = _connection.Name;
                txtDescription.Text = _connection.Description;

                txtEndpoint.Text = _connectionManager.Endpoint;
                txtIssuer.Text = _connectionManager.SharedAccessKeyName;
                txtSecret.Text = _connectionManager.SharedAccessKey;
                cmbMode.SelectedItem = _connectionManager.Mode.ToString();
                cmbReceiveMode.SelectedItem = _connectionManager.ReceiveMode.ToString();
                txtQueue.Text = _connectionManager.Queue;
                txtTopic.Text = _connectionManager.Topic;
                txtSubscription.Text = _connectionManager.Subscription;
            }

            Modes mode = (Modes)Enum.Parse(typeof(Modes), cmbMode.SelectedItem.ToString());

            grpQueue.Enabled = (mode == Modes.QUEUE);
            grpTopic.Enabled = (mode != Modes.QUEUE);
            txtSubscription.Enabled = (mode == Modes.SUBSCRIPTION);
        }

        public void Initialize(IServiceProvider serviceProvider, Microsoft.SqlServer.Dts.Runtime.ConnectionManager connectionManager, IErrorCollectionService errorCollector)
        {
            this._serviceProvider = serviceProvider;            
            this._errorCollector = errorCollector;

            if (connectionManager != null && connectionManager.InnerObject != null)
            {           
                this._connection = connectionManager;
                this._connectionManager = (ServiceBusConnectionManager)connectionManager.InnerObject;
            }
        }

        private void btnOK_Click(object sender, EventArgs e)
        {
            this.DialogResult = DialogResult.OK;
        }

        private void btnCancel_Click(object sender, EventArgs e)
        {
            this.DialogResult = DialogResult.Cancel;
        }

        private void btnTestConnection_Click(object sender, EventArgs e)
        {
            try
            {
                var namespaceMgr = NamespaceManager.CreateFromConnectionString(this.ConnectionString);

                switch (this.Mode)
                {
                    case (Modes.QUEUE):
                        if (!namespaceMgr.QueueExists(this.Queue))
                        {
                            System.Windows.Forms.MessageBox.Show(
                                string.Format("The {0} queue does not exist.", this.Queue), "Service Bus Connection Test");
                            return;
                        }

                        break;
                    case (Modes.TOPIC):
                        if (!namespaceMgr.TopicExists(this.Topic))
                        {
                            System.Windows.Forms.MessageBox.Show(
                                string.Format("The {0} topic does not exist.", this.Topic), "Service Bus Connection Test");
                            return;
                        }

                        break;
                    case (Modes.SUBSCRIPTION):
                        if (!namespaceMgr.SubscriptionExists(this.Topic, this.Subscription))
                        {
                            System.Windows.Forms.MessageBox.Show(
                                string.Format("The {0} subscription does not exist on the {1} topic.", this.Subscription, this.Topic), 
                                "Service Bus Connection Test");
                            return;
                        }
                        
                        break;
                    default:
                        break;
                }

                System.Windows.Forms.MessageBox.Show("Connection Successful!", "Service Bus Connection Test");
            }
            catch (Exception)
            {
                System.Windows.Forms.MessageBox.Show("Connection failed due to invalid endpoint or credentials.", "Service Bus Connection Test");
            }
        }

        #region Properties
        private string ConnectionString
        {
            get
            {
                return String.Format(CultureInfo.InvariantCulture,
                                     "Endpoint={0};SharedAccessKeyName={1};SharedAccessKey={2}",
                                     Endpoint,
                                     SharedAccessKeyName,
                                     SharedAccessKey);
            }
        }

        public string ConnectionName { get { return txtName.Text;  } }
        public string Description { get { return txtDescription.Text;  } }
        public string Endpoint { get { return txtEndpoint.Text;  } }
        public string SharedAccessKeyName { get { return txtIssuer.Text;  } }
        public string SharedAccessKey { get { return txtSecret.Text;  } }
        public string Queue { get { return txtQueue.Text;  } }
        public string Topic { get { return txtTopic.Text;  } }
        public string Subscription { get { return txtSubscription.Text;  } }
        public ReceiveMode ReceiverMode { get { return (ReceiveMode)Enum.Parse(typeof(ReceiveMode), (string)cmbReceiveMode.SelectedItem); } }
        public Modes Mode { get { return (Modes)Enum.Parse(typeof(Modes), (string)cmbMode.SelectedItem); } }
        public bool CreateWhenNotExists { get { return chkCreate.Checked; } }
        #endregion

    }
}
