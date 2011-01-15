using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;

namespace DataMigratorTest
{
	class Program
	{
		[System.Data.Linq.Mapping.Table(Name="user")]
		public class MySqlUser
		{
			[System.Data.Linq.Mapping.Column]
			public string User
			{
				get;
				set;
			}

			[System.Data.Linq.Mapping.Column]
			public int Password
			{
				get;
				set;
			}

			[System.Data.Linq.Mapping.Column]
			public string Host
			{
				get;
				set;
			}

			[System.Data.Linq.Mapping.Column]
			public int Foo
			{
				get;
				set;
			}
		}

		static void Main(string[] args)
		{
			MySqlConnection conn = new MySqlConnection("Data Source=192.168.1.102; DataBase=test; Uid=root; Pwd=nignog; Port=3306;");
			conn.Open();

			var dx = new DbLinq.MySql.MySqlDataContext(conn);

			var m = dx.GetDataMigrator();
			m.AddTable<MySqlUser>();

			m.Migrate();
		}
	}
}
