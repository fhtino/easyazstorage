using System;
using System.Collections.Generic;
using System.Text;

namespace EasyAzStorage
{
    public class AzureStorage
    {
        private string _connString;
        private string _prefix;
        private EasyTable _tables;
        private EasyQueue _queues;

        public AzureStorage(string connString, string prefix = null)
        {
            _connString = connString;
            _prefix = prefix;
            _tables = new EasyTable(connString, prefix);
            _queues = new EasyQueue();
        }


        public EasyTable Tables { get { return _tables; } }
        public EasyQueue Queues { get { return _queues; } }
        // TODO : blobs


    }
}
