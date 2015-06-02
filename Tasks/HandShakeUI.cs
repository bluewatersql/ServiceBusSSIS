using System;
using Microsoft.DataTransformationServices.Controls;
using Microsoft.SqlServer.Dts.Runtime;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Dts.Runtime.Design;
using System.Windows.Forms;

namespace BluewaterSQL.DTS.ServiceBus.Tasks
{
    public class HandShakeUI : IDtsTaskUI
    {
        private TaskHost _taskHost = null;
        private IDtsConnectionService _connectionService = null;
        private IServiceProvider _serviceProvider = null;
        #region IDtsTaskUI Members

        public void Delete(IWin32Window parentWindow)
        {
        }

        public ContainerControl GetView()
        {
            return new HandShakeUIMainWnd(_taskHost, _serviceProvider, _connectionService);
        }

        public void Initialize(TaskHost taskHost, IServiceProvider serviceProvider)
        {
            this._taskHost = taskHost;
            this._serviceProvider = serviceProvider;
            this._connectionService = serviceProvider.GetService(typeof(IDtsConnectionService)) as IDtsConnectionService;
        }

        public void New(IWin32Window parentWindow)
        {
        }
        #endregion
    }
}
