using Microsoft.DataTransformationServices.Controls;
using Microsoft.SqlServer.Dts.Runtime;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;

namespace BluewaterSQL.DTS.ServiceBus.Tasks
{
    public partial class HandShakeUIMainWnd : DTSBaseTaskUI
    {
        private IServiceProvider serviceProvider = null;

        // UI properties
        private const string Title = "HandShake Task";
        private const string Description = "Performs the HandShake for a Brokered Message";
        private static Icon TaskIcon = new Icon(typeof(HandShake).Assembly.GetManifestResourceStream("BluewaterSQL.DTS.ServiceBus.Tasks.Task.ico"));

        private HandShakeGeneralView generalView;
        public HandShakeGeneralView GeneralView
        {
            get { return generalView; }
        }

        public HandShakeUIMainWnd(TaskHost taskHost, IServiceProvider serviceProvider, object connections) :
            base(Title, TaskIcon, Description, taskHost, connections)
        {            
            InitializeComponent();

            this.serviceProvider = serviceProvider;

            // Setup our views
            generalView = new HandShakeGeneralView();
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
