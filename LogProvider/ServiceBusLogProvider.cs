using Microsoft.ServiceBus.Messaging;
using Microsoft.SqlServer.Dts.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LogProvider
{
    [DtsLogProvider(DisplayName = "Service Bus Log Provider", Description = "Sends SSIS log messages to the configured Queue or Topic.", LogProviderType = "Custom")]
    public class ServiceBusLogProvider : LogProviderBase
    {
        Connections _connections = null;
        MessageClientEntity _client = null;

        public override void InitializeLogProvider(Connections connections, IDTSInfoEvents events, ObjectReferenceTracker refTracker)
        {
            _connections = connections;
        }

        public override DTSExecResult Validate(IDTSInfoEvents infoEvents)
        {
            bool bValid = true;

            if (string.IsNullOrEmpty(this.ConfigString) || _connections.Contains(ConfigString) == false)
            {
                infoEvents.FireError(-1, null, "The connection manager " + ConfigString + " specified in the ConfigString property cannot be found in the collection.", null, 0);
                bValid = false;
            }
            else
            {
                var conn = _connections[ConfigString].AcquireConnection(null);

                if (!(conn is QueueClient) && !(conn is TopicClient))
                {
                    infoEvents.FireError(-1, null, "Invalid connection manager type specified for Log Provider.", null, 0);
                    bValid = false;
                }

            }
            return (bValid == true) ? DTSExecResult.Success : DTSExecResult.Failure;
        }

        public override void OpenLog()
        {
            _client = _connections[ConfigString].AcquireConnection(null) as MessageClientEntity;

            if (_client == null)
                throw new Exception("Invalid connection manager specified for Log Provider");
        }

        public override void Log(string logEntryName, string computerName, string operatorName, string sourceName, string sourceID, string executionID, string messageText, DateTime startTime, DateTime endTime, int dataCode, byte[] dataBytes)
        {
            BrokeredMessage logMessage = new BrokeredMessage();

            logMessage.Properties.Add(new KeyValuePair<string, object>("LogEntryName", logEntryName));
            logMessage.Properties.Add(new KeyValuePair<string, object>("ComputerName", computerName));
            logMessage.Properties.Add(new KeyValuePair<string, object>("OperatorName", operatorName));
            logMessage.Properties.Add(new KeyValuePair<string, object>("SourceName", sourceName));
            logMessage.Properties.Add(new KeyValuePair<string, object>("SourceID", sourceID));
            logMessage.Properties.Add(new KeyValuePair<string, object>("ExecutionID", executionID));
            logMessage.Properties.Add(new KeyValuePair<string, object>("MessageText", messageText));
            logMessage.Properties.Add(new KeyValuePair<string, object>("StartTime", startTime));
            logMessage.Properties.Add(new KeyValuePair<string, object>("EndTime", endTime));
            logMessage.Properties.Add(new KeyValuePair<string, object>("DataCode", dataCode));

            if (_client is QueueClient)
                ((QueueClient)_client).SendAsync(logMessage);
            else if (_client is TopicClient)
                ((TopicClient)_client).SendAsync(logMessage);
        }
    }
}
