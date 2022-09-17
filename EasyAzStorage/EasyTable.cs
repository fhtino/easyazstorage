using Azure;
using Azure.Data.Tables;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace easyazstorage
{

    public class EasyTable
    {

        // Reference:  https://github.com/Azure/azure-sdk-for-net/blob/main/sdk/tables/Azure.Data.Tables/README.md


        private string _connectionString;
        private string _prefix;


        public EasyTable(string connectionString, string prefix)
        {
            _connectionString = connectionString;
            _prefix = prefix ?? string.Empty;
        }


        public void CreateTableIfNotExist<T>() where T : ITableEntity
        {
            this.GetAzureTableClient<T>(true);
        }


        public void Save<T>(T obj) where T : class, ITableEntity, new()
        {
            var tableClient = this.GetAzureTableClient<T>();
            tableClient.UpsertEntity(obj);
        }


        public int SaveMultiBatch<T>(List<T> entities) where T : class, ITableEntity, new()
        {
            int batchCounter = 0;

            var groupByPK = entities.GroupBy(x => x.PartitionKey);

            foreach (var group in groupByPK)
            {
                string pk = group.Key;

                List<T> allPKitems = group.ToList();

                for (int i = 0; i < Math.Ceiling(allPKitems.Count / 100.0); i++)
                {
                    var batch = allPKitems.Skip(i * 100).Take(100).ToList();
                    if (batch.Count > 0)
                    {
                        SaveBacthTransaction(batch);
                        batchCounter++;
                    }
                }
            }

            return batchCounter;
        }


        public void SaveBacthTransaction<T>(List<T> entities) where T : class, ITableEntity, new()
        {
            var tableClient = this.GetAzureTableClient<T>();

            var batch = new List<TableTransactionAction>();

            batch.AddRange(entities.Select(item => new TableTransactionAction(TableTransactionActionType.UpsertMerge, item)));

            Azure.Response<IReadOnlyList<Azure.Response>> responses = tableClient.SubmitTransaction(batch);

            // AFAIK, it's not required to check every single result. In case of error, SubmitTransaction throws an exception.

            //foreach (var resp in responses.Value)
            //{
            //    if (resp.IsError)
            //        throw new ApplicationException("ERROR");
            //}            
        }


        public void Delete<T>(T obj, bool throwIfNotFound = false) where T : class, ITableEntity, new()
        {

            var tableClient = this.GetAzureTableClient<T>();
            var resp = tableClient.DeleteEntity(obj.PartitionKey, obj.RowKey);

            if (resp.IsError)
            {
                if (resp.Status == 404)
                {
                    if (throwIfNotFound)
                        throw new ApplicationException("Item not found (http 404)");
                }
                else
                {
                    throw new ApplicationException($"ERROR: {resp.ToString()}");
                }
            }

        }


        public void DeleteBatchTransaction<T>(List<T> entities) where T : class, ITableEntity, new()
        {
            // note: tableClient.SubmitTransaction is already optimized. It only sends pk, rk and etag over the wire.

            var tableClient = this.GetAzureTableClient<T>();

            var batch = new List<TableTransactionAction>();

            batch.AddRange(
                entities.Select(
                    item => new TableTransactionAction(TableTransactionActionType.Delete, item)));

            Azure.Response<IReadOnlyList<Azure.Response>> responses = tableClient.SubmitTransaction(batch);
        }


        public int DeleteMultiBatch<T>(List<T> entities) where T : class, ITableEntity, new()
        {
            //ResetCounters();

            int batchCounter = 0;

            var groupByPK = entities.GroupBy(x => x.PartitionKey);

            foreach (var group in groupByPK)
            {
                string pk = group.Key;

                List<T> allPKitems = group.ToList();

                for (int i = 0; i < Math.Ceiling(allPKitems.Count / 100.0); i++)
                {
                    var batch = allPKitems.Skip(i * 100).Take(100).ToList();
                    if (batch.Count > 0)
                    {
                        DeleteBatchTransaction(batch);
                        //LastRunNumOfItems += batch.Count;
                        //LastRunNumOfBatches++;
                    }
                }
            }

            return batchCounter;
        }


        //private void ResetCounters()
        //{
        //    LastRunNumOfItems = 0;
        //    LastRunNumOfBatches = 0;
        //}

        //public int LastRunNumOfItems { get; private set; }
        //public int LastRunNumOfBatches { get; private set; }










        public T Retrieve<T>(string pk, string rk) where T : class, ITableEntity, new()
        {
            var tableClient = this.GetAzureTableClient<T>();

            try
            {
                var obj = tableClient.GetEntity<T>(pk, rk);
                return obj;
            }
            catch (Azure.RequestFailedException ex)
            {
                if (ex.Status == 404)
                    return null;
                else
                    throw;
            }
        }



        //public T[] ParallelRetrieve<T>(List<PKRK> pkrkList) where T : ITableEntity
        //{
        //    pkrkList = pkrkList.Distinct(new PKRKEqComparer()).ToList();
        //    ConcurrentBag<T> resultList = new ConcurrentBag<T>();
        //    var cloudtable = _azureStorage.GetAzureTable<T>();
        //    Parallel.ForEach(pkrkList,
        //        //new ParallelOptions() { MaxDegreeOfParallelism = 5 },  // per debug
        //        item =>
        //        {
        //            TableOperation retrieveOperation = TableOperation.Retrieve<T>(item.PK, item.RK);
        //            var res = cloudtable.Execute(retrieveOperation);
        //            if (res.Result != null)
        //                resultList.Add((T)res.Result);
        //        });
        //    return resultList.ToArray();
        //}


        public List<T> RetrieveParallel<T>((string PK, string RK)[] pkList) where T : class, ITableEntity, new()
        {
            var tableClient = this.GetAzureTableClient<T>();

            var buffer = new ConcurrentBag<T>();

            Parallel.ForEach(pkList,
                item =>
                {
                    var obj = this.Retrieve<T>(item.PK, item.RK);
                    buffer.Add(obj);

                });

            return buffer.ToList();
        }



        public List<T> RunQuery<T>(Expression<Func<T, bool>> filter, int? topN = null) where T : class, ITableEntity, new()
        {
            // Currently Azure.Data.Tables does not implement "TopN"
            // https://github.com/Azure/azure-sdk-for-net/issues/30985

            if (filter == null)
            {
                // this generates a query without a "where" condition
                filter = item => true;
            }

            TableClient tableClient = this.GetAzureTableClient<T>();

            // Remember: every underlying REST API call returns 1000 items (maximum).
            // Giving an explicit maxPerPages is not strictly required.
            // But if TopN < 1000, it would be a waste of resource retrieving more items than required.            
            int? maxPerPages = (topN.HasValue && topN < 1000) ? topN : null;

            var query = tableClient.Query<T>(filter, maxPerPages);

            var outputBuffer = new List<T>();

            foreach (var page in query.AsPages())
            {
                outputBuffer.AddRange(page.Values);

                if (topN.HasValue && outputBuffer.Count >= topN.Value)
                    break;
            }

            if (topN.HasValue)
                outputBuffer = outputBuffer.Take(topN.Value).ToList();

            return outputBuffer;
        }



        public T First<T>() where T : class, ITableEntity, new()
        {
            var list = RunQuery<T>(null, 1);
            return list.FirstOrDefault();
        }









        internal TableClient GetAzureTableClient<T>(bool createIfNotExists = false, string explicitTableName = null)
        {
            string tableName;

            Attribute[] attrs = Attribute.GetCustomAttributes(typeof(T));

            //Attribute att = attrs.FirstOrDefault(a => a.GetType() == typeof(FhtinoAzureStorage.CustomTableMapping));
            //if (att != null)
            //{
            //    tabName = _prefix + ((FhtinoAzureStorage.CustomTableMapping)att).TableName;
            //}
            //else
            //{
            //    tabName = _prefix + typeof(T).Name;
            //}

            tableName = _prefix + typeof(T).Name;

            if (explicitTableName != null)
            {
                tableName = explicitTableName;
            }

            tableName = tableName.ToLower();

            var tableService = new TableServiceClient(_connectionString);

            if (createIfNotExists)
                tableService.CreateTableIfNotExists(tableName);

            return tableService.GetTableClient(tableName);
        }





    }
}
