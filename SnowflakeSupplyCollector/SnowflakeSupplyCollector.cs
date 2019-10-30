using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using S2.BlackSwan.SupplyCollector;
using S2.BlackSwan.SupplyCollector.Models;
using Snowflake.Data.Client;

namespace SnowflakeSupplyCollector {
    public class SnowflakeSupplyCollector : SupplyCollectorBase {
        public override List<string> DataStoreTypes() {
            return (new[] {"Snowflake"}).ToList();
        }

        public static string BuildConnectionString(string account, string region, string db, string user,
            string password) {
            return
                $"ACCOUNT={account};HOST={account}.{region}.snowflakecomputing.com;DB={db};USER={user};PASSWORD={password}";
        }


        public override List<string> CollectSample(DataEntity dataEntity, int sampleSize) {
            var result = new List<string>();

            using (IDbConnection conn = new SnowflakeDbConnection()) {
                conn.ConnectionString = dataEntity.Container.ConnectionString;
                conn.Open();

                var cmd = conn.CreateCommand();
                cmd.CommandText =
                    $"SELECT {dataEntity.Name} from {dataEntity.Collection.Schema}.{dataEntity.Collection.Name} sample row ({sampleSize} rows)";

                var reader = cmd.ExecuteReader();
                while (reader.Read()) {
                    if (!reader.IsDBNull(0))
                        result.Add(reader.GetValue(0).ToString());
                }
            }

            return result;
        }

        public override List<DataCollectionMetrics> GetDataCollectionMetrics(DataContainer container) {
            var metrics = new List<DataCollectionMetrics>();

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = container.ConnectionString;
                conn.Open();

                var cmd = conn.CreateCommand();
                cmd.CommandText =
                    $"SELECT table_catalog, table_schema, table_name, row_count, bytes FROM information_schema.tables";

                var reader = cmd.ExecuteReader();
                while (reader.Read()) {
                    var schema = reader.GetString(1);
                    var name = reader.GetString(2);
                    var rowCount = reader.IsDBNull(3) ? 0 : reader.GetInt64(3);
                    var bytes = reader.IsDBNull(4) ? 0 : reader.GetInt64(4);

                    metrics.Add(new DataCollectionMetrics() {
                        Name = name,
                        RowCount = rowCount,
                        Schema = schema,
                        TotalSpaceKB = bytes / 1024,
                        UsedSpaceKB = bytes / 1024,
                    });
                }
            }

            return metrics;
        }
        
        private DataType ConvertDataType(string dbDataType)
        {
            if ("INTEGER".Equals(dbDataType))
            {
                return DataType.Long;
            }
            else if ("SMALLINT".Equals(dbDataType))
            {
                return DataType.Short;
            }
            else if ("BOOLEAN".Equals(dbDataType))
            {
                return DataType.Boolean;
            }
            else if ("CHARACTER".Equals(dbDataType))
            {
                return DataType.Char;
            }
            else if ("VARCHAR".Equals(dbDataType))
            {
                return DataType.String;
            }
            else if ("TEXT".Equals(dbDataType))
            {
                return DataType.String;
            }
            else if ("STRING".Equals(dbDataType))
            {
                return DataType.String;
            }
            else if ("DOUBLE PRECISION".Equals(dbDataType))
            {
                return DataType.Double;
            }
            else if ("NUMBER".Equals(dbDataType))
            {
                return DataType.Decimal;
            }
            else if ("DECIMAL".Equals(dbDataType))
            {
                return DataType.Decimal;
            }
            else if ("NUMERIC".Equals(dbDataType))
            {
                return DataType.Decimal;
            }
            else if ("DATE".Equals(dbDataType))
            {
                return DataType.DateTime;
            }
            else if ("TIME".Equals(dbDataType))
            {
                return DataType.DateTime;
            }
            else if ("TIMESTAMP".Equals(dbDataType))
            {
                return DataType.DateTime;
            }
            else if ("TIMESTAMP_LTZ".Equals(dbDataType))
            {
                return DataType.DateTime;
            }
            else if ("TIMESTAMP_NTZ".Equals(dbDataType))
            {
                return DataType.DateTime;
            }
            else if ("TIMESTAMP_TZ".Equals(dbDataType))
            {
                return DataType.DateTime;
            }
            else if ("DATETIME".Equals(dbDataType))
            {
                return DataType.DateTime;
            }

            return DataType.Unknown;
        }

        public override (List<DataCollection>, List<DataEntity>) GetSchema(DataContainer container) {
            var collections = new List<DataCollection>();
            var entities = new List<DataEntity>();

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = container.ConnectionString;
                conn.Open();

                var cmd = conn.CreateCommand();
                cmd.CommandText =
                    $"SELECT table_schema, table_name, ordinal_position, column_name, data_type FROM information_schema.columns order by table_schema, table_name, ordinal_position";

                DataCollection coll = null;
                var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    var schema = reader.GetString(0);
                    var tableName = reader.GetString(1);
                    var columnName = reader.GetString(3);
                    var dataType = reader.GetString(4);

                    if (coll == null || !coll.Name.Equals(tableName) || !coll.Schema.Equals(schema)) {
                        coll = new DataCollection(container, tableName) {
                            Schema = schema
                        };
                        collections.Add(coll);
                    }

                    entities.Add(new DataEntity(columnName, ConvertDataType(dataType), dataType, container, coll));
                }
            }

            return (collections, entities);
        }

        public override bool TestConnection(DataContainer container)
        {
            try
            {
                using (IDbConnection conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString = container.ConnectionString;
                    conn.Open();
                }

                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }
    }
}