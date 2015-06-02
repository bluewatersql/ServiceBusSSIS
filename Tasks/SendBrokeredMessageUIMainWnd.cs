using Microsoft.DataTransformationServices.Controls;
using Microsoft.SqlServer.Dts.Runtime;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;

namespace BluewaterSQL.DTS.ServiceBus.Tasks
{
    public partial class SendBrokeredMessageUIMainWnd : DTSBaseTaskUI
    {
        private IServiceProvider serviceProvider = null;

        // UI properties
        private const string Title = "Send Brokered Message Task";
        private const string Description = "Sends a brokered message to the configured connection";
        private static Icon TaskIcon = new Icon(typeof(HandShake).Assembly.GetManifestResourceStream("BluewaterSQL.DTS.ServiceBus.Tasks.Task.ico"));

        private SendBrokeredMessageGeneralView generalView;

        public SendBrokeredMessageGeneralView GeneralView
        {
            get { return generalView; }
        }

        public SendBrokeredMessageUIMainWnd(TaskHost taskHost, IServiceProvider serviceProvider, object connections) :
            base(Title, TaskIcon, Description, taskHost, connections)
        {            
            InitializeComponent();

            this.serviceProvider = serviceProvider;

            // Setup our views
            generalView = new SendBrokeredMessageGeneralView();
            this.DTSTaskUIHost.FastLoad = false;
            this.DTSTaskUIHost.AddView("General", generalView, null);
            this.DTSTaskUIHost.FastLoad = true;
        }

        #region Designer code
        private System.ComponentModel.IContainer components = null;

        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        private void InitializeComponent()
        {
            this.components = new System.ComponentModel.Container();
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
        }
        #endregion
    }
}
