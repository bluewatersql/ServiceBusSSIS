using Microsoft.DataTransformationServices.Controls;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Runtime.Design;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace BluewaterSQL.DTS.ServiceBus.Tasks
{
    public class SendBrokeredMessageGeneralView : UserControl, IDTSTaskUIView
    {
        private PropertyGrid propertyGrid;
        private GeneralViewNode generalNode;

        public SendBrokeredMessageGeneralView()
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

            SendBrokeredMessage task = host.InnerObject as SendBrokeredMessage;
            if (task == null)
            {
                throw new ArgumentException("Argument is not a Send Brokered Message.", "taskHost");
            }

            host.Name = generalNode.Name;
            host.Description = generalNode.Description;

            // Task properties            
            task.SourceType = generalNode.SourceType;
            task.ConnectionName = generalNode.Connection;
            task.UseAsync = generalNode.UseAsync;

            if (task.SourceType == SourceType.Variable)
                task.Source = generalNode.Source;
            else if (task.SourceType == SourceType.Direct)
                task.Source = generalNode.SourceDirect;
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
        [SortProperties(new string[] { "Connection", "SourceType", "Source", "SourceDirect" })]
        internal class GeneralViewNode : ICustomTypeDescriptor
        {
            // Properties variables
            private string source;
            private string sourceDirect;
            private SourceType sourceType = SourceType.Direct;
            private string connection;
            private bool useAsync;
            private string name;
            private string description;

            internal IDtsConnectionService iDtsConnection;
            internal TaskHost myTaskHost;
            private IDtsVariableService variableService;

            internal IDtsVariableService VariableService
            {
                get
                {
                    return this.variableService;
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
                this.variableService = this.myTaskHost.Site.GetService(typeof (IDtsVariableService)) as IDtsVariableService;

                // Extract common values from the Task Host
                name = taskHost.Name;
                description = taskHost.Description;

                // Extract values from the task object
                SendBrokeredMessage task = taskHost.InnerObject as SendBrokeredMessage;
                if (task == null)
                {
                    string msg = string.Format(CultureInfo.CurrentCulture, "Type mismatch for taskHost inner object. Received: {0} Expected: {1}", taskHost.InnerObject.GetType().Name, typeof(HandShake).Name);
                    throw new ArgumentException(msg);
                }

                sourceType = task.SourceType;
                connection = task.ConnectionName;
                useAsync = task.UseAsync;

                if (sourceType == Tasks.SourceType.Direct)
                    sourceDirect = task.Source;
                else
                    source = task.Source;
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

            [Category("General"), Description("Service Bus Connection")]
            [TypeConverter(typeof(ConnectionTypeConverter))]
            [RefreshProperties(RefreshProperties.All)]
            public string Connection
            {
                get { return connection; }
                set { connection = value;  }
            }

            [Category("General"), Description("Variable or Source for Brokered Message")]
            [TypeConverter(typeof(SendBrokeredMessageVariableTypeConverter))]
            [RefreshProperties(RefreshProperties.All)]
            public string Source
            {
                get { return source; }
                set { source = value; }
            }

            [Category("General"), Description("Variable or Source for Brokered Message")]
            [RefreshProperties(RefreshProperties.All)]
            public string SourceDirect
            {
                get { return sourceDirect; }
                set { sourceDirect = value; }
            }

            [Category("General"), Description("Source Type")]
            [RefreshProperties(RefreshProperties.All)]
            public SourceType SourceType
            {
                get { return sourceType; }
                set { sourceType = value;  }
            }

            [Category("General"), Description("Use Asynchronous Communications")]
            public bool UseAsync
            {
                get { return useAsync;  }
                set { useAsync = value;  }
            }
            #endregion

            public PropertyDescriptorCollection GetProperties(Attribute[] attributes)
            {
                Attribute[] attributeArray = new Attribute[1];
                PropertyDescriptorCollection properties1 = TypeDescriptor.GetProperties((object)this, attributes, true);
                PropertyDescriptor[] propertyDescriptorArray = new PropertyDescriptor[properties1.Count];
                PropertyDescriptor[] properties2 = new PropertyDescriptor[properties1.Count];
                properties1.CopyTo((Array)propertyDescriptorArray, 0);
                properties1.CopyTo((Array)properties2, 0);
            
                Hashtable hashtable = new Hashtable();
                for (int index = 0; index < propertyDescriptorArray.Length; ++index)
                    hashtable.Add((object)propertyDescriptorArray[index].Name, (object)index);
            
                if (this.SourceType == Tasks.SourceType.Direct)
                {
                    attributeArray[0] = (Attribute)new BrowsableAttribute(false);
                    properties2[(int)hashtable["Source"]] = TypeDescriptor.CreateProperty(typeof(SendBrokeredMessageGeneralView.GeneralViewNode), properties1["Source"], attributeArray);
                }
                else
                {
                    attributeArray[0] = (Attribute)new BrowsableAttribute(false);
                    properties2[(int)hashtable["SourceDirect"]] = TypeDescriptor.CreateProperty(typeof(SendBrokeredMessageGeneralView.GeneralViewNode), properties1["SourceDirect"], attributeArray);
                }

                return new PropertyDescriptorCollection(properties2);
            }

            public TypeConverter GetConverter()
            {
                return TypeDescriptor.GetConverter((object)this, true);
            }

            public EventDescriptorCollection GetEvents(Attribute[] attributes)
            {
                return TypeDescriptor.GetEvents((object)this, attributes, true);
            }

            public EventDescriptorCollection GetEvents()
            {
                return TypeDescriptor.GetEvents((object)this, true);
            }

            public string GetComponentName()
            {
                return TypeDescriptor.GetComponentName((object)this, true);
            }

            public object GetPropertyOwner(PropertyDescriptor pd)
            {
                return (object)this;
            }

            public AttributeCollection GetAttributes()
            {
                return TypeDescriptor.GetAttributes((object)this, true);
            }

            public PropertyDescriptorCollection GetProperties()
            {
                return this.GetProperties(new Attribute[0]);
            }

            public object GetEditor(System.Type editorBaseType)
            {
                return TypeDescriptor.GetEditor((object)this, editorBaseType, true);
            }

            public PropertyDescriptor GetDefaultProperty()
            {
                return TypeDescriptor.GetDefaultProperty((object)this, true);
            }

            public EventDescriptor GetDefaultEvent()
            {
                return TypeDescriptor.GetDefaultEvent((object)this, true);
            }

            public string GetClassName()
            {
                return TypeDescriptor.GetClassName((object)this, true);
            }
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
            if (e.ChangedItem.PropertyDescriptor.Name.Equals("Source"))
            {
                if (this.generalNode.SourceType == SourceType.Variable)
                {
                    if (e.ChangedItem.Value.Equals("New Variable"))
                    {
                        this.Cursor = Cursors.WaitCursor;

                        this.generalNode.Source = null;

                        IDtsVariableService variableService = this.generalNode.VariableService;

                        if (variableService != null)
                        {
                            Variable variable = variableService.PromptAndCreateVariable((IWin32Window)this, (DtsContainer)this.generalNode.DtrTaskHost);

                            if ((DtsObject)variable == null)
                                this.generalNode.Source = (string)e.OldValue;
                            else if (!variable.ReadOnly && variable.DataType == TypeCode.Object)
                            {
                                this.generalNode.Source = variable.QualifiedName;
                            }
                            else
                            {
                                this.generalNode.Source = (string)e.OldValue;
                            }
                            this.Cursor = Cursors.Default;
                        }
                    }
                }
            }
            else if (e.ChangedItem.PropertyDescriptor.Name.Equals("Connection"))
            {
                if (e.ChangedItem.Value.Equals("New Connection"))
                {
                    this.Cursor = Cursors.WaitCursor;

                    this.generalNode.Connection = null;

                    ArrayList connection = this.generalNode.iDtsConnection.CreateConnection("MSOLAP100");

                    this.Cursor = Cursors.Default;
                    if (connection != null && connection.Count > 0)
                        this.generalNode.Connection = ((Microsoft.SqlServer.Dts.Runtime.ConnectionManager)connection[0]).Name;
                    else if (e.OldValue == null)
                        this.generalNode.Connection = null;
                    else
                        this.generalNode.Connection = (string)e.OldValue;
                }
            }
        }

        #endregion
    }
}
