namespace NpgsqlRepro
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Dapper;
    using Npgsql;
    using Xunit;
    using Xunit.Abstractions;

    public class UnitTest1
    {
        private const string PostgresDb = "postgres";
        private const string Database = nameof(NpgsqlRepro);
        private const string User = "postgres";
        private const string Password = "Password12!";

        private const string Schema = nameof(NpgsqlRepro) + nameof(Schema);

        private const int CommandSecondsTimeout = 10;
        private const IsolationLevel TestIsolationLevel = IsolationLevel.ReadCommitted;

        public UnitTest1(ITestOutputHelper output)
        {
            Output = output;
        }

        public ITestOutputHelper Output { get; }
    
        [Fact]
        public async Task Test1()
        {
            var timeout = TimeSpan.FromSeconds(60);
        
            using (var cts = new CancellationTokenSource(timeout))
            {
                // #1 - create or get database
                await CreateOrGetExistedDatabase(cts.Token).ConfigureAwait(false);
                
                // #2 - create model
                await CreateModel(cts.Token).ConfigureAwait(false);

                // #3 - insert entity
                var primaryKey = Guid.NewGuid();
                var affectedRows = await InsertEntity(primaryKey, cts.Token).ConfigureAwait(false);
                Assert.Equal(1, affectedRows);
                
                // #4 - read and assert entity
                var entity = await ReadEntity(primaryKey, cts.Token).ConfigureAwait(false);
                Assert.NotNull(entity);
                Assert.Equal(primaryKey, entity.PrimaryKey);
                Assert.Equal(1, entity.Version);

                // #5 - update entity
                affectedRows = await UpdateEntity(primaryKey, 1, cts.Token).ConfigureAwait(false);
                Assert.Equal(1, affectedRows);
                
                entity = await ReadEntity(primaryKey, cts.Token).ConfigureAwait(false);
                Assert.NotNull(entity);
                Assert.Equal(primaryKey, entity.PrimaryKey);
                Assert.Equal(2, entity.Version);

                // #6 - update entity concurrently
                var delay = TimeSpan.FromMilliseconds(1000);
                var sync = new AutoResetEvent(true);
                ConcurrentUpdateException? concurrentUpdateException;

                try
                {
                    await Task.WhenAll(
                            UpdateEntityConcurrently(primaryKey, 2, delay, sync, cts.Token),
                            UpdateEntityConcurrently(primaryKey, 2, delay, sync, cts.Token))
                       .ConfigureAwait(false);

                    concurrentUpdateException = default;
                }
                catch (ConcurrentUpdateException exception)
                {
                    concurrentUpdateException = exception;
                }
                
                Assert.NotNull(concurrentUpdateException);
                
                entity = await ReadEntity(primaryKey, cts.Token).ConfigureAwait(false);
                Assert.NotNull(entity);
                Assert.Equal(primaryKey, entity.PrimaryKey);
                Assert.Equal(3, entity.Version);
                
                // #7 - delete entity
                affectedRows = await DeleteEntity(primaryKey, cts.Token).ConfigureAwait(false);
                Assert.Equal(1, affectedRows);
            }
        }

        private static async Task<IDbConnection> OpenDbConnection(
            string database,
            CancellationToken token)
        {
            var connectionStringBuilder = new NpgsqlConnectionStringBuilder
            {
                Host = "localhost",
                Port = 5432,
                Database = database,
                Username = User,
                Password = Password,
                Pooling = true,
                MinPoolSize = 10,
                MaxPoolSize = 42,
                ConnectionPruningInterval = 1,
                ConnectionIdleLifetime = 1,
                IncludeErrorDetail = true
            };

            var npgSqlConnection = new NpgsqlConnection(connectionStringBuilder.ConnectionString);
        
            await npgSqlConnection
               .OpenAsync(token)
               .ConfigureAwait(false);

            return npgSqlConnection;
        }

        private static async Task CreateOrGetExistedDatabase(CancellationToken token)
        {
            using (var connection = await OpenDbConnection(PostgresDb, token).ConfigureAwait(false))
            {
                var command = new CommandDefinition(
                    CreateOrGetExistedDatabaseCommandText(),
                    null,
                    null,
                    CommandSecondsTimeout,
                    CommandType.Text,
                    CommandFlags.Buffered,
                    token);

                _ = await connection
                   .ExecuteAsync(command)
                   .ConfigureAwait(false);
            }

            NpgsqlConnection.ClearAllPools();
        }

        private static string CreateOrGetExistedDatabaseCommandText()
        {
            return string.Format(@"
create extension if not exists dblink;

do
$do$
    begin
        if exists
            (select * from pg_catalog.pg_database where datname = '{0}')
        then
            raise notice 'database already exists';
        else
            PERFORM dblink_connect('host=localhost user=' || '{1}' || ' password=' || '{2}' || ' dbname=' || current_database());
            perform dblink_exec('create database ""{0}""');
            perform dblink_exec('grant all privileges on database ""{0}"" to ""{1}""');
        end if;
    end
$do$;",
                Database,
                User,
                Password);
        }

        private static async Task CreateModel(CancellationToken token)
        {
            using (var connection = await OpenDbConnection(Database, token).ConfigureAwait(false))
            using (var transaction = connection.BeginTransaction(TestIsolationLevel))
            {
                var command = new CommandDefinition(
                    CreateModelCommandText(),
                    null,
                    transaction,
                    CommandSecondsTimeout,
                    CommandType.Text,
                    CommandFlags.Buffered,
                    token);

                _ = await connection
                   .ExecuteAsync(command)
                   .ConfigureAwait(false);
                    
                transaction.Commit();
            }
        }

        private static string CreateModelCommandText()
        {
            var createSchema = @$"create schema if not exists ""{Schema}""";
            var createTable = @$"create table if not exists ""{Schema}"".""{nameof(Entity)}"" (""{nameof(Entity.PrimaryKey)}"" uuid not null primary key, ""{nameof(Entity.Version)}"" bigint not null)";

            return string.Join(";" + Environment.NewLine, createSchema, createTable);
        }

        private static async Task<long> InsertEntity(Guid primaryKey, CancellationToken token)
        {
            using (var connection = await OpenDbConnection(Database, token).ConfigureAwait(false))
            using (var transaction = connection.BeginTransaction(TestIsolationLevel))
            {
                var command = new CommandDefinition(
                    InsertEntityCommandText(primaryKey),
                    null,
                    transaction,
                    CommandSecondsTimeout,
                    CommandType.Text,
                    CommandFlags.Buffered,
                    token);

                var affectedRows = await connection
                   .ExecuteAsync(command)
                   .ConfigureAwait(false);
                    
                transaction.Commit();

                return affectedRows;
            }
        }

        private static string InsertEntityCommandText(Guid primaryKey)
        {
            return @$"insert into ""{Schema}"".""{nameof(Entity)}""(""{nameof(Entity.PrimaryKey)}"", ""{nameof(Entity.Version)}"") values ('{primaryKey}', 1)";
        }

        private static async Task<Entity> ReadEntity(Guid primaryKey, CancellationToken token)
        {
            using (var connection = await OpenDbConnection(Database, token).ConfigureAwait(false))
            using (var transaction = connection.BeginTransaction(TestIsolationLevel))
            {
                var command = new CommandDefinition(
                    ReadEntityCommandText(primaryKey),
                    null,
                    transaction,
                    CommandSecondsTimeout,
                    CommandType.Text,
                    CommandFlags.Buffered,
                    token);

                var dynamicValues = await connection
                   .QueryAsync(command)
                   .ConfigureAwait(false);

                var values = (dynamicValues.SingleOrDefault() as IDictionary<string, object?>) !;

                if (values == default)
                {
                    throw new InvalidOperationException($"Entity {primaryKey} was not found");
                }

                var entity = new Entity
                {
                    PrimaryKey = (Guid)values[nameof(Entity.PrimaryKey)],
                    Version = (long)values[nameof(Entity.Version)]
                };
                    
                transaction.Rollback();

                return entity;
            }
        }

        private static string ReadEntityCommandText(Guid primaryKey)
        {
            return @$"select * from ""{Schema}"".""{nameof(Entity)}"" where ""{nameof(Entity.PrimaryKey)}"" = '{primaryKey}'";
        }

        private static async Task<long> UpdateEntity(Guid primaryKey, long version, CancellationToken token)
        {
            using (var connection = await OpenDbConnection(Database, token).ConfigureAwait(false))
            using (var transaction = connection.BeginTransaction(TestIsolationLevel))
            {
                var command = new CommandDefinition(
                    UpdateEntityCommandText(primaryKey, version),
                    null,
                    transaction,
                    CommandSecondsTimeout,
                    CommandType.Text,
                    CommandFlags.Buffered,
                    token);

                var affectedRows = await connection
                   .ExecuteAsync(command)
                   .ConfigureAwait(false);
                    
                transaction.Commit();

                return affectedRows;
            }
        }

        private static async Task<long> UpdateEntityConcurrently(Guid primaryKey, long version, TimeSpan delay, AutoResetEvent sync, CancellationToken token)
        {
            try
            {
                long expectedAffectedRows;

                using (var connection = await OpenDbConnection(Database, token).ConfigureAwait(false))
                using (var transaction = connection.BeginTransaction(TestIsolationLevel))
                {
                    var command = new CommandDefinition(UpdateEntityCommandText(primaryKey, version),
                        null,
                        transaction,
                        CommandSecondsTimeout,
                        CommandType.Text,
                        CommandFlags.Buffered,
                        token);

                    expectedAffectedRows = await connection
                       .ExecuteAsync(command)
                       .ConfigureAwait(false);

                    // TODO: #1 - problem reveals here when we are trying to simulate long running transaction
                    await MimicLongRunningTransaction().ConfigureAwait(false);

                    transaction.Rollback();
                }

                // TODO: #2 - but it works here when transaction as short as possible
                /*await MimicLongRunningTransaction().ConfigureAwait(false);*/

                using (var connection = await OpenDbConnection(Database, token).ConfigureAwait(false))
                using (var transaction = connection.BeginTransaction(TestIsolationLevel))
                {
                    var command = new CommandDefinition(UpdateEntityCommandText(primaryKey, version),
                        null,
                        transaction,
                        CommandSecondsTimeout,
                        CommandType.Text,
                        CommandFlags.Buffered,
                        token);

                    var actualAffectedRows = await connection
                       .ExecuteAsync(command)
                       .ConfigureAwait(false);

                    if (actualAffectedRows != expectedAffectedRows)
                    {
                        throw new ConcurrentUpdateException("Concurrent update violation");
                    }

                    transaction.Commit();

                    return actualAffectedRows;
                }
            }
            finally
            {
                sync.Set();
            }
            
            async Task MimicLongRunningTransaction()
            {
                await Task
                   .Delay(delay, token)
                   .ConfigureAwait(false);

                sync.WaitOne();
            }
        }

        private static string UpdateEntityCommandText(Guid primaryKey, long version)
        {
            return @$"update ""{Schema}"".""{nameof(Entity)}"" set ""{nameof(Entity.Version)}"" = ""{nameof(Entity.Version)}"" + 1 where ""{nameof(Entity.PrimaryKey)}"" = '{primaryKey}' and ""{nameof(Entity.Version)}"" = {version}";
        }

        private static async Task<long> DeleteEntity(Guid primaryKey, CancellationToken token)
        {
            using (var connection = await OpenDbConnection(Database, token).ConfigureAwait(false))
            using (var transaction = connection.BeginTransaction(TestIsolationLevel))
            {
                var command = new CommandDefinition(
                    DeleteEntityCommandText(primaryKey),
                    null,
                    transaction,
                    CommandSecondsTimeout,
                    CommandType.Text,
                    CommandFlags.Buffered,
                    token);

                var affectedRows = await connection
                   .ExecuteAsync(command)
                   .ConfigureAwait(false);
                    
                transaction.Commit();

                return affectedRows;
            }
        }

        private static string DeleteEntityCommandText(Guid primaryKey)
        {
            return @$"delete from ""{Schema}"".""{nameof(Entity)}"" where ""{nameof(Entity.PrimaryKey)}"" = '{primaryKey}'";
        }

        private class Entity
        {
            public Guid PrimaryKey { get; set; }
            public long Version { get; set; }
        }

        private sealed class ConcurrentUpdateException : Exception
        {
            public ConcurrentUpdateException(string message)
                : base(message)
            {
            }
        }
    }
}