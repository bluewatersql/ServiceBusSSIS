﻿using Microsoft.SqlServer.Dts.Runtime;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;

namespace BluewaterSQL.DTS.ServiceBus.Tasks
{
    internal class ConnectionTypeConverter : StringConverter
    {
        private object GetSpecializedObject(object contextInstance)
        {
            return contextInstance;
        }

        public override TypeConverter.StandardValuesCollection GetStandardValues(ITypeDescriptorContext context)
        {
            return new TypeConverter.StandardValuesCollection((ICollection)this.getConnections(this.GetSpecializedObject(context.Instance)));
        }

        public override bool GetStandardValuesExclusive(ITypeDescriptorContext context)
        {
            return true;
        }

        public override bool GetStandardValuesSupported(ITypeDescriptorContext context)
        {
            return true;
        }

        private ArrayList getConnections(object retrievalObject)
        {
            Connections connections = ((SendBrokeredMessageGeneralView.GeneralViewNode)retrievalObject).iDtsConnection.GetConnections();
            ArrayList arrayList = new ArrayList();
            arrayList.Add("New Connection");
            foreach (var connectionManager in connections)
            {
                if (connectionManager.CreationName.CompareTo("AZURESVCBUS") == 0)
                    arrayList.Add((object)connectionManager.Name);
            }
            return arrayList;
        }
    }
}
