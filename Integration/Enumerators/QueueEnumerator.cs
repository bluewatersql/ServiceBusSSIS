using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BluewaterSQL.DTS.ServiceBus.Enumerators
{
    public class QueueEnumerator : IEnumerator
    {
        private QueueClient Connection { get; set; }
        private BrokeredMessage Msg { get; set; }        
        private int ServerTimeout { get; set; }
        private int MaxReceiveAttempts { get; set; }
        private bool AutoCompleteMessage { get; set; }
        private int BatchSize { get; set; }
        private int MaxMessages { get; set; }        
        private int SentMessages { get; set; }
        private List<BrokeredMessage> Messages { get; set; }
        private int index { get; set; }

        public IEnumerator GetEnumerator()
        {
            return (IEnumerator)this;
        }

        public QueueEnumerator(MessageClientEntity conn) :
            this(conn, 5, -1, -1, 3, false)
        {

        }

        public QueueEnumerator(MessageClientEntity conn, int timeout, int batch, int maxMessages):
            this(conn, timeout, batch, maxMessages, 3, false)
        {

        }

        public QueueEnumerator(MessageClientEntity conn, int timeout, int batch, int maxMessages, int receiveAttempts, bool autoComplete)
        {
            Connection = (QueueClient)conn;
            ServerTimeout = timeout;
            MaxReceiveAttempts = receiveAttempts;
            AutoCompleteMessage = autoComplete;
            BatchSize = batch;
            MaxMessages = maxMessages;
            Messages = new List<BrokeredMessage>();
        }

        public object Current
        {
            get 
            {
                
                return Msg; 
            }
        }

        public bool MoveNext()
        {
            if (MaxMessages != -1 && SentMessages >= MaxMessages)
                return false;

            if (BatchSize == -1)
            {
                Msg = Connection.Receive(new TimeSpan(0, 0, this.ServerTimeout));
            }
            else
            {
                if ((index == -1) || (index >= Messages.Count -1))
                {
                    Messages = Connection.ReceiveBatch(this.BatchSize,
                        new TimeSpan(0, 0, this.ServerTimeout)).ToList();

                    if (Messages.Count == 0)
                        return false;

                    index = -1;
                }

                index++;
                Msg = Messages[index];
            }

            if (Msg != null)
            {
                //Handle posion messages
                if (Msg.DeliveryCount > MaxReceiveAttempts)
                {
                    Msg.DeadLetterAsync();

                    return MoveNext();
                }
                else
                {
                    if (AutoCompleteMessage)
                    {
                        Msg.CompleteAsync();
                    }

                    SentMessages++;
                }
            }

            return (Msg != null);
        }

        public void Reset()
        {
            //Do Nothing
        }
    }
}
