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
                    {Name = "TEST_DATA_TYPES", RowCount = 1, TotalSpaceKB = 32},
                new DataCollectionMetrics()
                    {Name = "TEST_FIELD_NAMES", RowCount = 1, TotalSpaceKB = 32},
                new DataCollectionMetrics()
                    {Name = "TEST_INDEX", RowCount = 7, TotalSpaceKB = 32},
                new DataCollectionMetrics()
                    {Name = "TEST_INDEX_REF", RowCount = 2, TotalSpaceKB = 48},
                new DataCollectionMetrics()
                    {Name = "TEST_ARRAY_TYPES", RowCount = 1, TotalSpaceKB = 48}
            };

            var result = _instance.GetDataCollectionMetrics(_container);
            foreach (var metric in metrics)
            {
                var resultMetric = result.First<DataCollectionMetrics>(x => x.Name.Equals(metric.Name));
                Assert.NotNull(resultMetric);

                Assert.Equal(metric.RowCount, resultMetric.RowCount);
                //Assert.Equal(metric.TotalSpaceKB, resultMetric.TotalSpaceKB);
            }
        }

        [Fact]
        public void GetTableNamesTest()
        {
            var (tables, elements) = _instance.GetSchema(_container);
            tables = tables.Where(x => x.Name.StartsWith("TEST_")).ToList();
            elements = elements.Where(x => x.Collection.Name.StartsWith("TEST_")).ToList();

            Assert.Equal(5, tables.Count);
            Assert.Equal(25, elements.Count);

            var tableNames = new string[] { "test_data_types", "test_field_names", "test_index", "test_index_ref", "test_array_types" };
            foreach (var tableName in tableNames)
            {
                var table = tables.Find(x => x.Name.Equals(tableName, StringComparison.InvariantCultureIgnoreCase));
                Assert.NotNull(table);
            }
        }

        [Fact]
        public void DataTypesTest()
        {
            var (tables, elements) = _instance.GetSchema(_container);

            var dataTypes = new Dictionary<string, string>() {
                {"ID", "NUMBER"},
                {"CHAR_FIELD", "TEXT"},
                {"VARCHAR_FIELD", "TEXT"},
                {"STRING_FIELD", "TEXT"},
                {"BOOLEAN_FIELD", "BOOLEAN"},
                {"NUMBER_FIELD", "NUMBER"},
                {"DECIMAL_FIELD", "NUMBER"},
                {"DOUBLE_FIELD", "FLOAT"},
                {"DATE_FIELD", "DATE"},
                {"TIMESTAMP_FIELD", "TIMESTAMP_LTZ"}
            };

            var columns = elements.Where(x => x.Collection.Name.Equals("TEST_DATA_TYPES")).ToArray();
            Assert.Equal(dataTypes.Count, columns.Length);

            foreach (var column in columns)
            {
                Assert.Contains(column.Name, (IDictionary<string, string>)dataTypes);
                Assert.Equal(dataTypes[column.Name], column.DbDataType);
            }
        }

        [Fact]
        public void SpecialFieldNamesTest()
        {
            var (tables, elements) = _instance.GetSchema(_container);

            var fieldNames = new string[] { "ID", "LOW_CASE", "UPCASE", "CAMELCASE", "Table", "array", "SELECT" }; // first 4 without quotes are converted to upper case

            var columns = elements.Where(x => x.Collection.Name.Equals("TEST_FIELD_NAMES")).ToArray();
            Assert.Equal(fieldNames.Length, columns.Length);

            foreach (var column in columns)
            {
                Assert.Contains(column.Name, fieldNames);
            }
        }

        /*[Fact] // Attributes not supported
        public void AttributesTest()
        {
            var (tables, elements) = _instance.GetSchema(_container);

            var idFields = elements.Where(x => x.Collection.Name.StartsWith("TEST_") && x.Name.Equals("ID")).ToArray();
            Assert.Equal(5, idFields.Length);

            foreach (var idField in idFields)
            {
                Assert.Equal(DataType.Unknown, idField.DataType);
                Assert.True(idField.IsPrimaryKey);
            }

            var uniqueField = elements.Find(x => x.Name.Equals("NAME"));
            Assert.True(uniqueField.IsUniqueKey);

            var refField = elements.Find(x => x.Name.Equals("INDEX_ID"));
            Assert.True(refField.IsForeignKey);

            foreach (var column in elements)
            {

                if (string.IsNullOrEmpty(column.Schema) || column.Name.Equals("ID") || column.Name.Equals("NAME") || column.Name.Equals("INDEX_ID"))
                {
                    continue;
                }

                Assert.False(column.IsPrimaryKey);
                Assert.False(column.IsAutoNumber);
                Assert.False(column.IsForeignKey);
                Assert.False(column.IsIndexed);
            }
        }*/

        [Fact]
        public void CollectSampleTest()
        {
            var entity = new DataEntity("name", DataType.String, "varchar", _container,
                new DataCollection(_container, "test_index") {Schema = "test"});

            var samples = _instance.CollectSample(entity, 7);
            Assert.InRange(samples.Count, 5, 9);
            Assert.Contains("Wednesday", samples);
        }
    }
}
