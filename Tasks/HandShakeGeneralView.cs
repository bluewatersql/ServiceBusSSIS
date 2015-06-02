using Microsoft.DataTransformationServices.Controls;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Runtime.Design;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace BluewaterSQL.DTS.ServiceBus.Tasks
{
    public class HandShakeGeneralView : UserControl, IDTSTaskUIView
    {
        private PropertyGrid propertyGrid;
        private GeneralViewNode generalNode;

        public HandShakeGeneralView()
        {
            InitializeComponent();
        }

        #region IDTSTaskUIView Members
        public void OnCommit(object taskHost)
        {
            TaskHost host = taskHost as TaskHost;
            if (host == null)
            {
                throw new ArgumentException("Argument is not a TaskHost.", "taskHost");
            }

            HandShake task = host.InnerObject as HandShake;
            if (task == null)
            {
                throw new ArgumentException("Argument is not a HandShake.", "taskHost");
            }

            host.Name = generalNode.Name;
            host.Description = generalNode.Description;

            // Task properties
            task.BrokeredMessage = generalNode.BrokeredMessage;
            task.Type = generalNode.HandShakeType;
            task.UseAsync = generalNode.UseAsync;
        }

        public void OnInitialize(IDTSTaskUIHost treeHost, TreeNode viewNode, object taskHost, object connections)
        {
            this.generalNode = new GeneralViewNode(taskHost as TaskHost, connections as IDtsConnectionService);
            this.propertyGrid.SelectedObject = generalNode;
        }

        public void OnLoseSelection(ref bool bCanLeaveView, ref string reason)
        {
        }

        public void OnSelection()
        {
        }

        public void OnValidate(ref bool bViewIsValid, ref string reason)
        {
        }

        #endregion

        #region GeneralNode
        internal class GeneralViewNode
        {
            // Properties variables
            private string brokeredMessage = string.Empty;
            private string name = string.Empty;
            private string description = string.Empty;
            private HandShakeType type = HandShakeType.Complete;
            private bool useAsync = false;

            internal IDtsConnectionService iDtsConnection;
            internal TaskHost myTaskHost;
            private IDtsVariableService _variableService;

            internal IDtsVariableService VariableService
            {
                get
                {
                    return this._variableService;
                }
            }

            internal TaskHost DtrTaskHost
            {
                get
                {
                    return this.myTaskHost;
                }
            }

            internal GeneralViewNode(TaskHost taskHost, IDtsConnectionService connectionService)
            {
                this.iDtsConnection = connectionService;
                this.myTaskHost = taskHost;
                this._variableService = this.myTaskHost.Site.GetService(typeof (IDtsVariableService)) as IDtsVariableService;

                // Extract common values from the Task Host
                name = taskHost.Name;
                description = taskHost.Description;

                // Extract values from the task object
                HandShake task = taskHost.InnerObject as HandShake;
                if (task == null)
                {
                    string msg = string.Format(CultureInfo.CurrentCulture, "Type mismatch for taskHost inner object. Received: {0} Expected: {1}", taskHost.InnerObject.GetType().Name, typeof(HandShake).Name);
                    throw new ArgumentException(msg);
                }

                brokeredMessage = task.BrokeredMessage;
                type = task.Type;
                useAsync = task.UseAsync;
            }

            #region Properties

            [Category("General"), Description("Task name")]            
            public string Name
            {
                get
                {
                    return name;
                }
                set
                {
                    string v = value.Trim();
                    if (string.IsNullOrEmpty(v))
                    {
                        throw new ArgumentException("Task name cannot be empty");
                    }
                    name = v;
                }
            }

            [Category("General"), Description("Task description")]
            public string Description
            {
                get
                {
                    return description;
                }
                set
                {
                    description = value.Trim();
                }
            }

            [Category("General"), Description("Variable containing Service Bus Brokered Message")]
            [TypeConverter(typeof(HandShakeVariableTypeConverter))]
            public string BrokeredMessage
            {
                get { return brokeredMessage; }
                set { brokeredMessage = value; }
            }

            [Category("General")]
            public HandShakeType HandShakeType
            {
                get { return type; }
                set { type = value;  }
            }

            [Category("General")]
            public bool UseAsync
            {
                get { return useAsync; }
                set { useAsync = value; }
            }
            #endregion
        }

        #endregion

        #region Designer code
        private void InitializeComponent()
        {
            this.propertyGrid = new System.Windows.Forms.PropertyGrid();
            this.SuspendLayout();
            // 
            // propertyGrid
            // 
            this.propertyGrid.Dock = System.Windows.Forms.DockStyle.Fill;
            this.propertyGrid.Location = new System.Drawing.Point(0, 0);
            this.propertyGrid.Name = "propertyGrid";
            this.propertyGrid.PropertySort = System.Windows.Forms.PropertySort.Categorized;
            this.propertyGrid.Size = new System.Drawing.Size(150, 150);
            this.propertyGrid.TabIndex = 0;
            this.propertyGrid.ToolbarVisible = false;
            this.propertyGrid.PropertyValueChanged += propertyGrid_PropertyValueChanged;

            // 
            // GeneralView
            // 
            this.Controls.Add(this.propertyGrid);
            this.Name = "GeneralView";
            this.ResumeLayout(false);
        }

        private void propertyGrid_PropertyValueChanged(object s, PropertyValueChangedEventArgs e)
        {
            if (e.ChangedItem.PropertyDescriptor.Name.Equals("BrokeredMessage"))
            {
                if (e.ChangedItem.Value.Equals("New Variable"))
                {
                    this.Cursor = Cursors.WaitCursor;

                    this.generalNode.BrokeredMessage = null;

                    IDtsVariableService variableService = this.generalNode.VariableService;

                    if (variableService != null)
                    {
                        Variable variable = variableService.PromptAndCreateVariable((IWin32Window)this, (DtsContainer)this.generalNode.DtrTaskHost);

                        if ((DtsObject)variable == null)
                            this.generalNode.BrokeredMessage = (string)e.OldValue;
                        else if (!variable.ReadOnly && variable.DataType == TypeCode.Object)
                        {
                            this.generalNode.BrokeredMessage = variable.QualifiedName;
                        }
                        else
                        {
                            this.generalNode.BrokeredMessage = (string)e.OldValue;
                        }
                        this.Cursor = Cursors.Default;
                    }
                }
            }
        }

        #endregion
    }
}
