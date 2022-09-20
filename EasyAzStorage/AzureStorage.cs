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
        private EasyBlob _blobs;

        public AzureStorage(string connString, string prefix = null)
        {
            _connString = connString;
            _prefix = prefix;
            _tables = new EasyTable(connString, prefix);
            _queues = new EasyQueue();
            _blobs = new EasyBlob(connString);
        }


        public EasyTable Tables { get { return _tables; } }
        public EasyQueue Queues { get { return _queues; } }
        public EasyBlob Blobs { get { return _blobs; } }

    }
}
