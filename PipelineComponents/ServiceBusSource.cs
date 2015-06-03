using BluewaterSQL.DTS.ServiceBus.Enumerators;
using Microsoft.ServiceBus.Messaging;
using Microsoft.SqlServer.Dts.Pipeline;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;

namespace BluewaterSQL.DTS.ServiceBus.DataFlow
{
    [DtsPipelineComponent(DisplayName = "Service Bus Source", 
        Description = "Serializes business objects passed in BrokeredMessages to the SSIS Pipeline",
        IconResource = "BluewaterSQL.DTS.ServiceBus.DataFlow.Task.ico",
        ComponentType = ComponentType.SourceAdapter)]
    public class ServiceBusSource : PipelineComponent
    {
        #region Properties
        private MessageClientEntity client;
        //private Type messageBodyType;
        //private List<DTSColumn> objectColumns;
        private List<DTSOutputMetadata> DTSOutputs = new List<DTSOutputMetadata>();
        //private Dictionary<string, int> outputColumnToBufferMap;

        private bool bCancel;
        private bool isConnected;

        private string AssemblyName { get { return GetPropertyValue<string>("Assembly"); } }
        private string ClassName { get { return GetPropertyValue<string>("Class"); } }
        private int MaxStringLength { get { return GetPropertyValue<int>("MaxStringLength", s => int.Parse(s)); } }
        public int ServiceBusTimeout { get { return GetPropertyValue<int>("ServiceBusTimeout", s => int.Parse(s)); } }
        public int MessageCap { get { return GetPropertyValue<int>("MessageCap", s => int.Parse(s)); } }
        public int BatchSize { get { return GetPropertyValue<int>("BatchSize", s => int.Parse(s)); } }
        public int MaxRetrieveAttempts { get { return GetPropertyValue<int>("MaxRetrieveAttempts", s => int.Parse(s)); } }
        public bool AutoComplete { get { return GetPropertyValue<bool>("AutoComplete", s => bool.Parse(s)); } }
        #endregion

        #region Set-Up
        public override void ProvideComponentProperties()
        {
            base.RemoveAllInputsOutputsAndCustomProperties();
            ComponentMetaData.RuntimeConnectionCollection.RemoveAll();

            PipelineHelper.CreateProperty(ComponentMetaData, "Assembly", "Fully-qualified assembly name", "BluewaterSQL.DTS.ServiceBus.Integration, Version=1.0.0.0, Culture=neutral, PublicKeyToken=7675b8b105168229");
            PipelineHelper.CreateProperty(ComponentMetaData, "Class", "Class type for BrokeredMessage data", "BluewaterSQL.DTS.ServiceBus.Integration.Samples.Account");
            PipelineHelper.CreateProperty(ComponentMetaData, "MaxStringLength", "Default maximum string length", 50);

            PipelineHelper.CreateProperty(ComponentMetaData, "ServiceBusTimeout", "Service Bus Receive Timeout", 10);
            PipelineHelper.CreateProperty(ComponentMetaData, "MessageCap", "Maximum messages to recieved in a single unit of work", -1);
            PipelineHelper.CreateProperty(ComponentMetaData, "BatchSize", "Enables batch retrieval of BrokeredMessages", 50);
            PipelineHelper.CreateProperty(ComponentMetaData, "MaxRetrieveAttempts", "Maximum message retrieve attempts for posioned messages", 3);
            PipelineHelper.CreateProperty(ComponentMetaData, "AutoComplete", "Auto completes messages when retrieved", false);
            
            IDTSRuntimeConnection100 conn = ComponentMetaData.RuntimeConnectionCollection.New();
            conn.Name = "ServiceBusConnection";

            IDTSOutput100 output = ComponentMetaData.OutputCollection.New();
            output.Name = "Output";

            //output.Dangling = true;
            output.ExternalMetadataColumnCollection.IsUsed = true;
            output.ExclusionGroup = 0;
            output.SynchronousInputID = 0;

            this.ReinitializeMetaData();
        }

        public override void ReinitializeMetaData()
        {
            SetupOutputs(this.AssemblyName, this.ClassName);
            base.ReinitializeMetaData();
        }

        public override IDTSCustomProperty100 SetComponentProperty(string propertyName, object propertyValue)
        {
            IDTSCustomProperty100 property = base.SetComponentProperty(propertyName, propertyValue);

            SetupOutputs(this.AssemblyName, this.ClassName);

            return property;
        }
        #endregion

        #region Validate
        public override DTSValidationStatus Validate()
        {
            DTSValidationStatus validationStatus = base.Validate();

            if (validationStatus == DTSValidationStatus.VS_ISCORRUPT)
                return validationStatus;

            Assembly a = null;

            if (!string.IsNullOrEmpty(this.AssemblyName))
            {
                try
                {
                    a = Assembly.Load(this.AssemblyName);

                    if (a == null)
                    {
                        this.ComponentMetaData.FireError(-1, null, string.Format("Unable to load AssemblyName (NULL): {0}", this.AssemblyName), null, 0, out bCancel);
                        return DTSValidationStatus.VS_ISBROKEN;
                    }
                }
                catch (Exception)
                {
                    this.ComponentMetaData.FireError(-1, null, string.Format("Unable to load AssemblyName: {0}", this.AssemblyName), null, 0, out bCancel);
                    return DTSValidationStatus.VS_ISBROKEN;
                }
            }
            else
            {
                this.ComponentMetaData.FireError(-1, null, "AssemblyName", null, 0, out bCancel);
                return DTSValidationStatus.VS_ISBROKEN;
            }

            if (!string.IsNullOrEmpty(this.ClassName))
            {
                try
                {
                    dynamic c = a.CreateInstance(this.ClassName);

                    if (c == null)
                    {
                        this.ComponentMetaData.FireError(-1, null, string.Format("Unable to load ClassName (NULL): {0}", this.ClassName), null, 0, out bCancel);
                        return DTSValidationStatus.VS_ISBROKEN;
                    }
                }
                catch (Exception)
                {
                    this.ComponentMetaData.FireError(-1, null, string.Format("Unable to load ClassName: {0}", this.ClassName), null, 0, out bCancel);
                    return DTSValidationStatus.VS_ISBROKEN;
                }
            }
            else
            {
                this.ComponentMetaData.FireError(-1, null, "ClassName", null, 0, out bCancel);
                return DTSValidationStatus.VS_ISBROKEN;
            }

            if (this.MaxStringLength < 1)
            {
                this.ComponentMetaData.FireError(-1, null, "MaxStringLength must be greater than 1.", null, 0, out bCancel);
                return DTSValidationStatus.VS_ISBROKEN;
            }

            if (ComponentMetaData.ValidateExternalMetadata)
            {
                List<DTSColumnMetadata> dtsColumns = null;

                try
                {
                    Assembly assembly = Assembly.Load(this.AssemblyName);
                    dynamic obj = assembly.CreateInstance(this.ClassName);

                    dtsColumns = Flatten(obj.GetType().Name, obj);
                }
                catch (Exception)
                {
                    this.ComponentMetaData.FireError(-1, null, "Error Validating External Metadata", null, 0, out bCancel);
                    return DTSValidationStatus.VS_ISBROKEN;
                }

                IDTSOutput100 output = this.ComponentMetaData.OutputCollection[0];
                IDTSExternalMetadataColumnCollection100 columnCollection = output.ExternalMetadataColumnCollection;

                if (columnCollection.Count == 0)
                    return DTSValidationStatus.VS_NEEDSNEWMETADATA;

                bool validMetadata = true;

                for (int outputIndex = 0; outputIndex < output.OutputColumnCollection.Count; ++outputIndex)
                {
                    IDTSOutputColumn100 dtsOutputColumn = output.OutputColumnCollection[outputIndex];
                    IDTSExternalMetadataColumn100 objectById;

                    try
                    {
                        objectById = columnCollection.GetObjectByID(dtsOutputColumn.ExternalMetadataColumnID);
                    }
                    catch (COMException)
                    {
                        this.ComponentMetaData.FireError(-1, null, dtsOutputColumn.IdentificationString, null, 0, out bCancel);                        
                        return DTSValidationStatus.VS_NEEDSNEWMETADATA;
                    }

                    var dtsColumn = dtsColumns.SingleOrDefault(c => c.Name == objectById.Name);

                    if (dtsColumn != null)
                    {
                        if (dtsOutputColumn.DataType != dtsColumn.DataType ||
                            dtsOutputColumn.Length != dtsColumn.Length ||
                            dtsOutputColumn.Precision != dtsColumn.Precision ||
                            dtsOutputColumn.Scale != dtsColumn.Scale ||
                            dtsOutputColumn.CodePage != dtsColumn.CodePage)
                        {
                            this.ComponentMetaData.FireError(-1, null, dtsOutputColumn.IdentificationString, null, 0, out bCancel);
                            validMetadata = false;
                        }
                    }
                    else
                    {
                        this.ComponentMetaData.FireError(-1, null, dtsOutputColumn.IdentificationString, null, 0, out bCancel);
                        validMetadata = false;
                    }
                }

                if (!validMetadata)
                {
                    return DTSValidationStatus.VS_NEEDSNEWMETADATA;
                }
            }

            return validationStatus;
        }
        #endregion

        #region Connection Management
        public override void AcquireConnections(object transaction)
        {
            if (!isConnected)
            {
                IDTSRuntimeConnection100 runtimeConnection100;
                try
                {
                    runtimeConnection100 = this.ComponentMetaData.RuntimeConnectionCollection["ServiceBusConnection"];
                }
                catch (Exception)
                {
                    ComponentMetaData.FireError(-1, null, "Service Bus Connection not defined.", null, 0, out bCancel);
                    return;
                    //throw new Exception("Service Bus Connection not defined.");
                }

                IDTSConnectionManager100 connectionManager = runtimeConnection100.ConnectionManager;
                if (connectionManager == null)
                {
                    ComponentMetaData.FireError(-1, null, "Service Bus Connection Manager not defined.", null, 0, out bCancel);
                    return;
                    //throw new Exception("Service Bus Connection Manager not defined.");
                }
                else
                {
                    object obj;
                    try
                    {
                        obj = connectionManager.AcquireConnection(transaction);
                    }
                    catch (Exception)
                    {
                        ComponentMetaData.FireError(-1, null, "Failed to acquire connection to Service Bus Connection Manager.", null, 0, out bCancel);
                        return; 
                        //throw new Exception("Failed to acquire connection to Service Bus Connection Manager.");
                    }

                    this.client = obj as MessageClientEntity;

                    if (this.client == null || this.client is TopicClient)
                    {
                        ComponentMetaData.FireError(-1, null, "Unrecognized Service Bus Connection Manager type.", null, 0, out bCancel);
                        return; 
                        //throw new Exception("Unrecognized Service Bus Connection Manager type");
                    }
                    else
                    {
                        isConnected = true;
                    }
                }
            }
        }

        public override void ReleaseConnections()
        {
            if (this.client != null)
            {
                DtsConvert.GetWrapper(this.ComponentMetaData.RuntimeConnectionCollection["ServiceBusConnection"].ConnectionManager).ReleaseConnection(this.client);
                this.client = null;
            }
            isConnected = false;
        }
        #endregion

        #region Runtime
        public override void PreExecute()
        {
            base.PreExecute();

            Assembly assembly = Assembly.Load(this.AssemblyName);
            Type messageBodyType = assembly.GetType(this.ClassName);

            dynamic obj = assembly.CreateInstance(this.ClassName);

            IDTSOutput100 output = ComponentMetaData.OutputCollection[0];

            DTSOutputMetadata dtsOutput = new DTSOutputMetadata()
            {
                ID = output.ID,
                Name = output.Name,
                ObjectType = messageBodyType,
                Columns  = Flatten(obj.GetType().Name, obj)
            };

            for (int i = 0; i < output.OutputColumnCollection.Count; i++)
            {
                dtsOutput.OutputColumnToBufferMap[output.OutputColumnCollection[i].Name] = 
                    BufferManager.FindColumnByLineageID(output.Buffer, output.OutputColumnCollection[i].LineageID);
            }
        }

        public override void PrimeOutput(int outputs, int[] outputIDs, PipelineBuffer[] buffers)
        {
            if (this.client == null)
                ComponentMetaData.FireError(-1, null, "Service Bus Connection is closed.", null, 0, out bCancel);

            base.PrimeOutput(outputs, outputIDs, buffers);

            IDTSOutput100 output = ComponentMetaData.OutputCollection.FindObjectByID(outputIDs[0]);
            DTSOutputMetadata dtsOutput = this.DTSOutputs.SingleOrDefault(o => o.ID == output.ID);

            PipelineBuffer buffer = buffers[0];

            int rowCount = 0;

            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            IEnumerator enumerator = null;

            if (client is QueueClient)
                enumerator = new QueueEnumerator(this.client, this.ServiceBusTimeout, this.BatchSize, this.MessageCap, this.MaxRetrieveAttempts, this.AutoComplete);
            else if (client is TopicClient)
                enumerator = new SubscriptionEnumerator(this.client, this.ServiceBusTimeout, this.BatchSize, this.MessageCap, this.MaxRetrieveAttempts, this.AutoComplete);

            while (enumerator.MoveNext())
            {
                BrokeredMessage msg = enumerator.Current as BrokeredMessage;

                MethodInfo method = typeof(BrokeredMessage).GetMethod("GetBody", new Type[] { });
                MethodInfo generic = method.MakeGenericMethod(dtsOutput.ObjectType);

                try
                {
                    dynamic messageBody = generic.Invoke(msg, null);

                    buffer.AddRow();
                    ObjectToBuffer(dtsOutput, buffer, dtsOutput.ObjectType.Name, messageBody);

                    rowCount++;
                } 
                catch(Exception)
                {
                    ComponentMetaData.FireWarning(-1, null, string.Format("Unexpected Message Format (Message ID: {0})", msg.MessageId), null, 0);
                }
            }

            stopWatch.Stop();

            ComponentMetaData.IncrementPipelinePerfCounter(101U, (uint)rowCount);
            ComponentMetaData.FireInformation(0, 
                ((IDTSComponentMetaData100)this.ComponentMetaData).Name, 
                string.Format("Loaded {0} records from Service Bus<'{1}'>. Elapsed time is {2}ms", rowCount, this.ClassName, stopWatch.ElapsedMilliseconds), 
                null, 
                0,
                ref bCancel);

            buffer.SetEndOfRowset();

        }
        #endregion

        #region ObjectToBuffer
        private void ObjectToBuffer<T>(DTSOutputMetadata dtsOutput, PipelineBuffer buffer, string prefix, T obj)
        {
            Type type = typeof(T);

            foreach (PropertyInfo p in type.GetProperties())
            {
                if (!p.PropertyType.IsArray)
                {
                    if ((IsValidType(p.PropertyType) || p.PropertyType.IsEnum))
                    {
                        string columnName = string.Format("{0}_{1}", prefix, p.Name);
                        object value = p.GetValue(obj, new object[]{});

                        Transfer(dtsOutput, buffer, columnName, value);
                    }
                    else if (p.PropertyType.IsClass)
                    {
                        prefix = string.Format("{0}_{1}", prefix, p.Name);

                        //dynamic v = obj.GetType().Assembly.CreateInstance(p.PropertyType.FullName);
                        object value = p.GetValue(obj, new object[]{});
                        dynamic v = Convert.ChangeType(value, p.PropertyType);

                        ObjectToBuffer(dtsOutput, buffer, prefix, v);
                    }
                }
            }
        }
        #endregion

        #region Transfer
        private void Transfer(DTSOutputMetadata dtsOutput, PipelineBuffer buffer, string column, object value)
        {
            var dtsColumn = dtsOutput.Columns.SingleOrDefault(c => c.Name == column);
            int idx = dtsOutput.OutputColumnToBufferMap[column];

            if (value == null)
            {
                buffer.SetNull(idx);
                return;
            }

            try
            {
                switch (dtsColumn.DataType)
                {
                    case DataType.DT_STR:
                    case DataType.DT_WSTR:
                    case DataType.DT_NTEXT:
                        buffer.SetString(idx, value.ToString());
                        break;
                    case DataType.DT_NUMERIC:
                    case DataType.DT_CY:
                        buffer.SetDecimal(idx, Convert.ToDecimal(value));
                        break;
                    case DataType.DT_DBDATE:
                        buffer.SetDate(idx, Convert.ToDateTime(value));
                        break;
                    case DataType.DT_DBTIMESTAMP:
                    case DataType.DT_DBTIMESTAMP2:
                        buffer.SetDateTime(idx, Convert.ToDateTime(value));
                        break;
                    case DataType.DT_DBTIME:
                    case DataType.DT_DBTIME2:
                        buffer.SetTime(idx, (TimeSpan)value);
                        break;
                    case DataType.DT_DBTIMESTAMPOFFSET:
                        buffer.SetDateTimeOffset(idx, (DateTimeOffset)value);
                        break;
                    case DataType.DT_I2:
                        buffer.SetInt16(idx, Convert.ToInt16(value));
                        break;
                    case DataType.DT_I4:
                        buffer.SetInt32(idx, Convert.ToInt32(value));
                        break;
                    case DataType.DT_R4:
                        buffer.SetSingle(idx, (float)value);
                        break;
                    case DataType.DT_R8:
                        buffer.SetDouble(idx, Convert.ToDouble(value));
                        break;
                    case DataType.DT_BOOL:
                        buffer.SetBoolean(idx, Convert.ToBoolean(value));
                        break;
                    case DataType.DT_I1:
                        buffer.SetSByte(idx, (sbyte)value);
                        break;
                    case DataType.DT_UI1:
                        buffer.SetByte(idx, (byte)value);
                        break;
                    case DataType.DT_UI2:
                        buffer.SetUInt16(idx, (ushort)value);
                        break;
                    case DataType.DT_UI4:
                        buffer.SetUInt32(idx, (uint)value);
                        break;
                    case DataType.DT_I8:
                        buffer.SetInt64(idx, !(value is TimeSpan) ? (long)value : ((TimeSpan)value).Ticks);
                        break;
                    case DataType.DT_UI8:
                        buffer.SetUInt64(idx, (ulong)Convert.ToInt64(value));
                        break;
                    case DataType.DT_GUID:
                        buffer.SetGuid(idx, (Guid)value);
                        break;
                }
            }
            catch (Exception)
            {
                buffer.SetNull(idx);
            }
        }
        #endregion

        #region Set-up Outputs
        private void SetupOutputs(string assemblyName, string className)
        {
            IDTSOutput100 output = ComponentMetaData.OutputCollection[0];
            output.OutputColumnCollection.RemoveAll();

            Assembly assembly = Assembly.Load(assemblyName);
            dynamic obj = assembly.CreateInstance(className);

            List<DTSColumnMetadata> dtsColumns = Flatten(obj.GetType().Name, obj);

            foreach (DTSColumnMetadata dtsColumn in dtsColumns)
            {
                IDTSExternalMetadataColumn100 metadata = output.ExternalMetadataColumnCollection.New();
                metadata.Name = dtsColumn.Name;
                metadata.DataType = dtsColumn.DataType;
                metadata.Length = dtsColumn.Length;
                metadata.Precision = dtsColumn.Precision;
                metadata.Scale = dtsColumn.Scale;
                metadata.CodePage = dtsColumn.CodePage;

                IDTSOutputColumn100 column = output.OutputColumnCollection.New();

                column.Name = dtsColumn.Name;
                column.ExternalMetadataColumnID = metadata.ID;
                column.SetDataTypeProperties(dtsColumn.DataType, dtsColumn.Length, dtsColumn.Precision, dtsColumn.Scale, dtsColumn.CodePage);
            }
        }
        #endregion

        #region Flatten
        private List<DTSColumnMetadata> Flatten<T>(string prefix, T obj)
        {
            var columns = new List<DTSColumnMetadata>();

            Type type = typeof(T);

            foreach (PropertyInfo p in type.GetProperties())
            {
                if (!p.PropertyType.IsArray)
                {
                    if ((IsValidType(p.PropertyType) || p.PropertyType.IsEnum))
                    {
                        var d = new DTSColumnMetadata(prefix, p, this.MaxStringLength);
                        columns.Add(d);
                    }
                    else if (p.PropertyType.IsClass)
                    {
                        if (string.IsNullOrEmpty(prefix))
                            prefix = p.Name;
                        else
                            prefix = string.Format("{0}_{1}", prefix, p.Name);

                        dynamic v = obj.GetType().Assembly.CreateInstance(p.PropertyType.FullName);

                        var cols = Flatten(prefix, v);
                        columns.AddRange(cols);
                    }
                }
            }

            return columns;
        }

        private bool IsValidType(Type type)
        {
            bool bValid = false;

            switch (type.ToString())
            {
                case "System.Boolean":
                case "System.Byte":
                case "System.Char":
                case "System.DateTime":
                case "System.DateTimeOffset":
                case "System.Decimal":
                case "System.Double":
                case "System.Int16":
                case "System.Int32":
                case "System.Int64":
                case "System.SByte":
                case "System.Single":
                case "System.String":
                case "System.TimeSpan":
                case "System.UInt16":
                case "System.UInt32":
                case "System.UInt64":
                case "System.Guid":
                    bValid = true;
                    break;
                default:
                    bValid = false;
                    break;
            }

            return bValid;
        }
        #endregion

        #region Restricted Functions
        public override IDTSInput100 InsertInput(DTSInsertPlacement insertPlacement, int inputID)
        {
            throw new NotSupportedException();
        }

        public override IDTSOutput100 InsertOutput(DTSInsertPlacement insertPlacement, int outputID)
        {
            throw new NotSupportedException();
        }

        public override void DeleteInput(int inputID)
        {
            throw new NotSupportedException();
        }

        public override void DeleteOutput(int outputID)
        {
            throw new NotSupportedException();
        }

        public override IDTSOutputColumn100 InsertOutputColumnAt(int outputID, int outputColumnIndex, string name, string description)
        {
            throw new NotSupportedException();
        }

        public override void DeleteOutputColumn(int outputID, int outputColumnID)
        {
            throw new NotSupportedException();
        }
        #endregion

        #region Property Helper

        private T GetPropertyValue<T>(string propertyName, Func<string, T> converter = null)
        {
            if (converter == null)
            {
                return (T)ComponentMetaData.CustomPropertyCollection[propertyName].Value;
            }
            else
            {
                return converter(ComponentMetaData.CustomPropertyCollection[propertyName].Value.ToString());
            }
        }

        #endregion
    }
}
