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
                $"ACCOUNT={account};HOST={account}.{region}.snowflakecomputing.com;DB={db};USER={user},PASSWORD={password}";
        }


        public override List<string> CollectSample(DataEntity dataEntity, int sampleSize) {
            var result = new List<string>();

            using (IDbConnection conn = new SnowflakeDbConnection()) {
                conn.ConnectionString = dataEntity.Container.ConnectionString;
                conn.Open();

                var cmd = conn.CreateCommand();
                cmd.CommandText =
                    $"SELECT {dataEntity.Name} from {dataEntity.Collection.Name} sample row ({sampleSize} rows)";

                var reader = cmd.ExecuteReader();
                while (reader.Read()) {
                    if (reader.IsDBNull(0))
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
                    var rowCount = reader.GetInt64(3);
                    var bytes = reader.GetInt64(4);

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
            if ("integer".Equals(dbDataType))
            {
                return DataType.Long;
            }
            else if ("smallint".Equals(dbDataType))
            {
                return DataType.Short;
            }
            else if ("boolean".Equals(dbDataType))
            {
                return DataType.Boolean;
            }
            else if ("character".Equals(dbDataType))
            {
                return DataType.Char;
            }
            else if ("varchar".Equals(dbDataType))
            {
                return DataType.String;
            }
            else if ("text".Equals(dbDataType))
            {
                return DataType.String;
            }
            else if ("string".Equals(dbDataType))
            {
                return DataType.String;
            }
            else if ("double precision".Equals(dbDataType))
            {
                return DataType.Double;
            }
            else if ("number".Equals(dbDataType))
            {
                return DataType.Decimal;
            }
            else if ("decimal".Equals(dbDataType))
            {
                return DataType.Decimal;
            }
            else if ("numeric".Equals(dbDataType))
            {
                return DataType.Decimal;
            }
            else if ("date".Equals(dbDataType))
            {
                return DataType.DateTime;
            }
            else if ("time".Equals(dbDataType))
            {
                return DataType.DateTime;
            }
            else if ("timestamp".Equals(dbDataType))
            {
                return DataType.DateTime;
            }
            else if ("timestamp_ltz".Equals(dbDataType))
            {
                return DataType.DateTime;
            }
            else if ("timestamp_ntz".Equals(dbDataType))
            {
                return DataType.DateTime;
            }
            else if ("timestamp_tz".Equals(dbDataType))
            {
                return DataType.DateTime;
            }
            else if ("datetime".Equals(dbDataType))
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