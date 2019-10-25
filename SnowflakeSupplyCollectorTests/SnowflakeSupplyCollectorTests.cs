using System;
using System.Collections.Generic;
using System.Linq;
using S2.BlackSwan.SupplyCollector.Models;
using Xunit;

namespace SnowflakeSupplyCollectorTests
{
    public class SnowflakeSupplyCollectorTests : IClassFixture<LaunchSettingsFixture>
    {
        private readonly SnowflakeSupplyCollector.SnowflakeSupplyCollector _instance;
        public readonly DataContainer _container;
        private LaunchSettingsFixture _fixture;

        public SnowflakeSupplyCollectorTests(LaunchSettingsFixture fixture)
        {
            _fixture = fixture;
            _instance = new SnowflakeSupplyCollector.SnowflakeSupplyCollector();
            _container = new DataContainer()
            {
                ConnectionString = SnowflakeSupplyCollector.SnowflakeSupplyCollector.BuildConnectionString(
                    Environment.GetEnvironmentVariable("SNOWFLAKE_ACCOUNT"),
                    Environment.GetEnvironmentVariable("SNOWFLAKE_REGION"),
                    Environment.GetEnvironmentVariable("SNOWFLAKE_DB"),
                    Environment.GetEnvironmentVariable("SNOWFLAKE_USER"),
                    Environment.GetEnvironmentVariable("SNOWFLAKE_PASS")
                    )
            };
        }

        [Fact]
        public void DataStoreTypesTest()
        {
            var result = _instance.DataStoreTypes();
            Assert.Contains("Snowflake", result);
        }

        [Fact]
        public void TestConnectionTest()
        {
            var result = _instance.TestConnection(_container);
            Assert.True(result);
        }

        [Fact]
        public void GetDataCollectionMetricsTest()
        {
            var metrics = new DataCollectionMetrics[] {
                new DataCollectionMetrics()
                    {Name = "test_data_types", RowCount = 1, TotalSpaceKB = 32},
                new DataCollectionMetrics()
                    {Name = "test_field_names", RowCount = 1, TotalSpaceKB = 32},
                new DataCollectionMetrics()
                    {Name = "test_index", RowCount = 7, TotalSpaceKB = 32},
                new DataCollectionMetrics()
                    {Name = "test_index_ref", RowCount = 2, TotalSpaceKB = 48},
                new DataCollectionMetrics()
                    {Name = "test_array_types", RowCount = 1, TotalSpaceKB = 48}
            };

            var result = _instance.GetDataCollectionMetrics(_container);
            foreach (var metric in metrics)
            {
                var resultMetric = result.First<DataCollectionMetrics>(x => x.Name.Equals(metric.Name));
                Assert.NotNull(resultMetric);

                Assert.Equal(metric.RowCount, resultMetric.RowCount);
                Assert.Equal(metric.TotalSpaceKB, resultMetric.TotalSpaceKB);
            }
        }

        [Fact]
        public void GetTableNamesTest()
        {
            var (tables, elements) = _instance.GetSchema(_container);
            tables = tables.Where(x => x.Name.StartsWith("test_")).ToList();
            elements = elements.Where(x => x.Collection.Name.StartsWith("test_")).ToList();

            Assert.Equal(5, tables.Count);
            Assert.Equal(32, elements.Count);

            var tableNames = new string[] { "test_data_types", "test_field_names", "test_index", "test_index_ref", "test_array_types" };
            foreach (var tableName in tableNames)
            {
                var table = tables.Find(x => x.Name.Equals(tableName));
                Assert.NotNull(table);
            }
        }

        [Fact]
        public void DataTypesTest()
        {
            var (tables, elements) = _instance.GetSchema(_container);

            var dataTypes = new Dictionary<string, string>() {
                {"id", "integer"},
                {"char_field", "char"},
                {"varchar_field", "varchar"},
                {"string_field", "string"},
                {"boolean_field", "boolean"},
                {"number_field", "number"},
                {"decimal_field", "decimal"},
                {"double_field", "double"},
                {"date_field", "date"},
                {"timestamp_field", "timestamp"}
            };

            var columns = elements.Where(x => x.Collection.Name.Equals("test_data_types")).ToArray();
            Assert.Equal(dataTypes.Count, columns.Length);

            foreach (var column in columns)
            {
                Assert.Contains(column.Name, (IDictionary<string, string>)dataTypes);
                Assert.Equal(column.DbDataType, dataTypes[column.Name]);
            }
        }

        [Fact]
        public void SpecialFieldNamesTest()
        {
            var (tables, elements) = _instance.GetSchema(_container);

            var fieldNames = new string[] { "id", "low_case", "UPCASE", "CamelCase", "Table", "SELECT" }; // first 4 without quotes are converted to lower case

            var columns = elements.Where(x => x.Collection.Name.Equals("test_field_names")).ToArray();
            Assert.Equal(fieldNames.Length, columns.Length);

            foreach (var column in columns)
            {
                Assert.Contains(column.Name, fieldNames);
            }
        }

        [Fact]
        public void AttributesTest()
        {
            var (tables, elements) = _instance.GetSchema(_container);

            var idFields = elements.Where(x => x.Name.Equals("id")).ToArray();
            Assert.Equal(4, idFields.Length);

            foreach (var idField in idFields)
            {
                Assert.Equal(DataType.Unknown, idField.DataType);
                Assert.True(idField.IsPrimaryKey);
            }

            var uniqueField = elements.Find(x => x.Name.Equals("name"));
            Assert.True(uniqueField.IsUniqueKey);

            var refField = elements.Find(x => x.Name.Equals("index_id"));
            Assert.True(refField.IsForeignKey);

            foreach (var column in elements)
            {

                if (string.IsNullOrEmpty(column.Schema) || column.Name.Equals("id") || column.Name.Equals("name") || column.Name.Equals("index_id"))
                {
                    continue;
                }

                Assert.False(column.IsPrimaryKey);
                Assert.False(column.IsAutoNumber);
                Assert.False(column.IsForeignKey);
                Assert.False(column.IsIndexed);
            }
        }

        [Fact]
        public void CollectSampleTest()
        {
            var entity = new DataEntity("name", DataType.String, "varchar", _container,
                new DataCollection(_container, "test_index"));

            var samples = _instance.CollectSample(entity, 7);
            Assert.InRange(samples.Count, 5, 9);
            Assert.Contains("Wednesday", samples);
        }
    }
}
