using Microsoft.SqlServer.Dts.Runtime;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing.Design;
using System.Linq;
using System.Text;

namespace BluewaterSQL.DTS.ServiceBus.Tasks
{
    public abstract class TaskBase: Task
    {
        #region Members
        protected string _brokeredMessage;
        protected SourceType _sourceType;
        private string _connectionName;
        protected IDTSComponentEvents _events;
        #endregion

        #region Properties
        [Editor(typeof(DropDownEditor), typeof(UITypeEditor))]
        public virtual string ConnectionName
        {
            get
            {
                return this._connectionName;
            }
            set
            {
                this._connectionName = value;
                if (this._connectionName != null)
                    return;
                this._connectionName = string.Empty;
            }
        }
        #endregion

        #region Methods
        public override DTSExecResult Validate(Connections connections, VariableDispenser variableDispenser, IDTSComponentEvents events, IDTSLogging log)
        {
            return this.InternalValidate(connections, variableDispenser, events, log);
        }

        public override DTSExecResult Execute(Connections connections, VariableDispenser variableDispenser, IDTSComponentEvents events, IDTSLogging log, object transaction)
        {
            return this.InternalExecute(connections, variableDispenser, events, log, transaction);
        }
        #endregion

        #region Validate Methods
        protected virtual Microsoft.SqlServer.Dts.Runtime.ConnectionManager ValidateConnection(Connections connections)
        {
            if (this.ConnectionName == string.Empty)
            {
                this.FireError("Connection manager not specified");
                return null;
            }
            else
            {
                Microsoft.SqlServer.Dts.Runtime.ConnectionManager connectionManager = (Microsoft.SqlServer.Dts.Runtime.ConnectionManager)null;
                if (connections != null)
                {
                    try
                    {
                        connectionManager = connections[(object)this.ConnectionName];
                    }
                    catch (Exception ex)
                    {
                        this.FireError(ex.Message);
                    }
                }
                if ((DtsObject)connectionManager == (DtsObject)null)
                {
                    this.FireError("Unable to locate specified connection manager");
                    return null;
                }
                else
                {
                    if (!(connectionManager.CreationName != "AZURESVCBUS"))
                        return connectionManager;

                    this.FireError("Incorrect connection manager type specified");
                    return null;
                }
            }
        }
        #endregion

        #region Virtual Methods
        protected virtual DTSExecResult InternalValidate(Connections connections, VariableDispenser variableDispenser, IDTSComponentEvents events, IDTSLogging log)
        {
            return DTSExecResult.Success;
        }

        protected virtual DTSExecResult InternalExecute(Connections connections, VariableDispenser variableDispenser, IDTSComponentEvents events, IDTSLogging log, object transaction)
        {
            return DTSExecResult.Success;
        }
        #endregion

        #region Helper Methods
        protected void FireError(string message)
        {
            if (this._events == null)
                return;

            this._events.FireError(-1, null, message, null, 0);
        }
        #endregion
    }
}
