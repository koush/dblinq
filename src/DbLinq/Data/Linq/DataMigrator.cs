using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Linq;
using System.Data;
using DbLinq.Data.Linq;
using System.Data.Linq.Mapping;

namespace DbLinq.Data.Linq
{
	public class DataMigrator
	{
		DataContext mDataContext;
		internal DataMigrator(DataContext dataContext)
		{
			mDataContext = dataContext;
		}

		const string DBLINQ_METADATA_TABLE_NAME = "dblinq_metadata";
		const string DBLINQ_METADATA_ID_COLUMN_NAME = "id";
		const string DBLINQ_METADATA_TABLE_NAME_COLUMN_NAME = "table_name";
		const string DBLINQ_METADATA_MAPPED_NAME_COLUMN_NAME = "mapped_name";
		const string DBLINQ_METADATA_DBTYPE_COLUMN_NAME = "db_type";

		[Table(Name = DBLINQ_METADATA_TABLE_NAME)]
		private class DbLinqMetaData
		{
			[Column(IsPrimaryKey = true, Name = DBLINQ_METADATA_ID_COLUMN_NAME)]
			public int Id
			{
				get;
				set;
			}

			[Column(Name = DBLINQ_METADATA_TABLE_NAME_COLUMN_NAME)]
			public string TableName
			{
				get;
				set;
			}

			[Column(Name = DBLINQ_METADATA_MAPPED_NAME_COLUMN_NAME)]
			public string MappedName
			{
				get;
				set;
			}

			[Column(Name = DBLINQ_METADATA_DBTYPE_COLUMN_NAME)]
			public string DbType
			{
				get;
				set;
			}
		}

		HashSet<Type> mTypes = new HashSet<Type>();
		public void AddTable<T>()
		{
			AddTable(typeof(T));
		}

		public void AddTable(Type type)
		{
			mTypes.Add(type);
		}

		IEnumerable<DbLinqMetaData> GetExistingMetaData()
		{
			var sqlProvider = mDataContext.Vendor.SqlProvider;
			string ddl = string.Format("CREATE TABLE IF NOT EXISTS {0} ({1} INTEGER PRIMARY KEY NOT NULL {2}, {3} TEXT, {4} TEXT, {5} TEXT)",
				sqlProvider.GetTable(DBLINQ_METADATA_TABLE_NAME),
				sqlProvider.GetTable(DBLINQ_METADATA_ID_COLUMN_NAME),
				sqlProvider.GetAutoIncrement(),
				sqlProvider.GetTable(DBLINQ_METADATA_TABLE_NAME_COLUMN_NAME),
				sqlProvider.GetTable(DBLINQ_METADATA_MAPPED_NAME_COLUMN_NAME),
				sqlProvider.GetTable(DBLINQ_METADATA_DBTYPE_COLUMN_NAME));

			var cmd = mDataContext.Connection.CreateCommand();
			cmd.CommandText = ddl;
			cmd.ExecuteNonQuery();

			return from metadata in mDataContext.GetTable<DbLinqMetaData>() select metadata;
		}

		string GetMemberDDL(MetaDataMember member)
		{
			var sqlProvider = mDataContext.Vendor.SqlProvider;
			string ddl = string.Format("{0} {1} {2} {3}", member.MappedName, sqlProvider.GetColumnType(member.Type), member.IsPrimaryKey ? "PRIMARY KEY" : string.Empty, !member.CanBeNull ? "NOT NULL" : string.Empty);
			return ddl;
		}

		DbLinqMetaData MemberToMetaData(MetaDataMember member)
		{
			var sqlProvider = mDataContext.Vendor.SqlProvider;
			return new DbLinqMetaData
								{
									TableName = member.DeclaringType.Table.TableName,
									MappedName = member.MappedName,
									DbType = member.Type.Name
								};
		}

		public void Migrate()
		{
			var connection = mDataContext.Connection;
			var sqlProvider = mDataContext.Vendor.SqlProvider;
			var metadata = GetExistingMetaData();

			var dblinq_metadata = mDataContext.GetTable<DbLinqMetaData>();

			Dictionary<string, Dictionary<string, string>> existingMetaData = new Dictionary<string, Dictionary<string, string>>();

			foreach (var entry in metadata)
			{
				Dictionary<string, string> tableColumns;
				if (!existingMetaData.TryGetValue(entry.TableName, out tableColumns))
				{
					tableColumns = new Dictionary<string,string>();
					existingMetaData.Add(entry.TableName, tableColumns);
				}

				tableColumns.Add(entry.MappedName, entry.DbType);
			}

			foreach (var type in mTypes)
			{
				var metaType = mDataContext.Mapping.GetMetaType(type);

				Dictionary<string,string> tableColumns;
				// let's find tables and columns currently missing from the database model
				if (!existingMetaData.TryGetValue(metaType.Table.TableName, out tableColumns))
				{
					// create the table, it's not there at all.
					var columnDdls = string.Join(",", (from member in metaType.DataMembers select GetMemberDDL(member)).ToArray());
					string ddl = string.Format("CREATE TABLE {0} ({1})", sqlProvider.GetTable(metaType.Table.TableName), columnDdls);
					var command = connection.CreateCommand();
					command.CommandText = ddl;
					command.ExecuteNonQuery();
					dblinq_metadata.BulkInsert(from member in metaType.DataMembers select MemberToMetaData(member));
					continue;
				}

				foreach (var member in metaType.DataMembers)
				{
					string fieldType;
					if (tableColumns.TryGetValue(member.MappedName, out fieldType))
					{
						if (fieldType == member.Type.Name)
							continue;
						throw new InvalidOperationException(string.Format("Error migrating {0}. Changing the type of {1} from {2} to {3} is not supported.", metaType.Name, member.Name, fieldType, member.Type.Name));
					}

					// add the column
					var command = connection.CreateCommand();
					command.CommandText = string.Format("ALTER TABLE {0} ADD COLUMN {1}", sqlProvider.GetTable(metaType.Table.TableName), GetMemberDDL(member));
					command.ExecuteNonQuery();

					dblinq_metadata.InsertOnSubmit(MemberToMetaData(member));
					mDataContext.SubmitChanges();
				}
			}
		}
	}
}
