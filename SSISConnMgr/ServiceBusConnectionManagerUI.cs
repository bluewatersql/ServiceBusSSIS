using Microsoft.SqlServer.Dts.Design;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Runtime.Design;
using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace BluewaterSQL.DTS.ServiceBus.ConnectionManager
{
    public class ServiceBusConnectionManagerUI: IDtsConnectionManagerUI
    {
        private Microsoft.SqlServer.Dts.Runtime.ConnectionManager _connectionManager;
        private IServiceProvider _serviceProvider;
        private IErrorCollectionService _errorCollectionService;
        private IDesignerHost _designerHost;

        public Microsoft.SqlServer.Dts.Runtime.ConnectionManager ConnectionManager
        {
            get { return this._connectionManager; }
        }

        public IServiceProvider ServiceProvider
        {
            get { return this._serviceProvider; }
        }

        public IErrorCollectionService ErrorCollectionService
        {
            get
            {
                if (this._errorCollectionService == null)
                    this._errorCollectionService = this.ServiceProvider.GetService(typeof(IErrorCollectionService)) as IErrorCollectionService;
                return this._errorCollectionService;
            }
        }

        public IDesignerHost DesignerHost
        {
            get
            {
                if (this._designerHost == null)
                    this._designerHost = this.ServiceProvider.GetService(typeof(IDesignerHost)) as IDesignerHost;
                return this._designerHost;
            }
        }

        void IDtsConnectionManagerUI.Delete(IWin32Window parentWindow)
        {
        }

        bool IDtsConnectionManagerUI.Edit(IWin32Window parentWindow, Connections connections, ConnectionManagerUIArgs connectionUIArg)
        {
            return this.ServiceBusConnectionManager(parentWindow, connections);
        }

        void IDtsConnectionManagerUI.Initialize(Microsoft.SqlServer.Dts.Runtime.ConnectionManager connectionManager, IServiceProvider serviceProvider)
        {
            this._serviceProvider = serviceProvider;
            this._connectionManager = connectionManager;
        }

        bool IDtsConnectionManagerUI.New(IWin32Window parentWindow, Connections connections, ConnectionManagerUIArgs connectionUIArg)
        {
            IDtsClipboardService clipboardService = this._serviceProvider.GetService(typeof(IDtsClipboardService)) as IDtsClipboardService;
            if (clipboardService != null && clipboardService.IsPasteActive)
                return true;
            else
                return this.ServiceBusConnectionManager(parentWindow, connections);
        }

        private bool ServiceBusConnectionManager(IWin32Window parentWindow, Connections connections)
        {
            using (ServiceBusConnectionManagerForm serviceBusConnectionManagerForm = new ServiceBusConnectionManagerForm())
            {
                serviceBusConnectionManagerForm.Initialize(this._serviceProvider, this.ConnectionManager, this.ErrorCollectionService);

                if (serviceBusConnectionManagerForm.ShowDialog(parentWindow) == DialogResult.OK)
                {
                    ServiceBusConnectionManager connMgr = (ServiceBusConnectionManager)this.ConnectionManager.InnerObject;

                    this.ConnectionManager.Name = serviceBusConnectionManagerForm.ConnectionName;
                    this.ConnectionManager.Description = serviceBusConnectionManagerForm.Description;

                    connMgr.Endpoint = serviceBusConnectionManagerForm.Endpoint;
                    connMgr.SharedAccessKeyName = serviceBusConnectionManagerForm.SharedAccessKeyName;
                    connMgr.SharedAccessKey = serviceBusConnectionManagerForm.SharedAccessKey;
                    connMgr.Mode = serviceBusConnectionManagerForm.Mode;
                    connMgr.ReceiveMode = serviceBusConnectionManagerForm.ReceiverMode;
                    connMgr.CreateWhenNotExists = serviceBusConnectionManagerForm.CreateWhenNotExists;

                    connMgr.Queue = string.Empty;
                    connMgr.Topic = string.Empty;
                    connMgr.Subscription = string.Empty;

                    switch (connMgr.Mode)
                    {
                        case(Modes.QUEUE):
                            connMgr.Queue = serviceBusConnectionManagerForm.Queue;
                            break;
                        case (Modes.TOPIC):
                            connMgr.Topic = serviceBusConnectionManagerForm.Topic;
                            break;
                        case (Modes.SUBSCRIPTION):
                            connMgr.Topic = serviceBusConnectionManagerForm.Topic;
                            connMgr.Subscription = serviceBusConnectionManagerForm.Subscription;
                            break;
                        default:
                            break;
                    }
                    return true;
                }
            }
            return false;
        }
    }
}
