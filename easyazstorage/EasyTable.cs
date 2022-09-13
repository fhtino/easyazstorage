using Azure.Data.Tables;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

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


        public void SaveBatch<T>(List<T> entities) where T : class, ITableEntity, new()
        {
            var tableClient = this.GetAzureTableClient<T>();

            var batch = new List<TableTransactionAction>();

            batch.AddRange(entities.Select(item => new TableTransactionAction(TableTransactionActionType.UpsertMerge, item)));

            Azure.Response<IReadOnlyList<Azure.Response>> response = tableClient.SubmitTransaction(batch);


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
