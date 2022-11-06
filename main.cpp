
#include "MySQL.h"
#include "helpers.h"

#include <cstdio>
#include <cstdarg>
#include <cassert>
#include <cstdlib>

using CppMySQL::MySQL;
using CppMySQL::MySQLStmt;
using CppMySQL::MySQLResult;
using CppMySQL::MySQLStmtResult;

static void __pr_err(const char *file, unsigned line, const char *func,
		     const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	printf("%s:%u %s(): ", file, line, func);
	vprintf(fmt, ap);
	printf("\n");
	va_end(ap);
}

#define pr_err(...)						\
do {								\
	__pr_err(__FILE__, __LINE__, __func__, __VA_ARGS__);	\
} while (0)

static MySQL *init_conn(void)
{
	const char *err;
	const char *host = getenv("CPPMYSQL_DB_HOST");
	const char *user = getenv("CPPMYSQL_DB_USER");
	const char *pass = getenv("CPPMYSQL_DB_PASS");
	const char *dbname = getenv("CPPMYSQL_DB_NAME");
	const char *port_s = getenv("CPPMYSQL_DB_PORT");
	uint16_t port = (port_s ? (uint16_t)atoi(port_s) : 0);
	MySQL *mysql;

	mysql = new MySQL(host, user, pass, dbname, port);
	if (mysql->Init())
		goto err;
	if (mysql->Connect())
		goto err;

	return mysql;
err:
	err = mysql->Error();
	pr_err("MySQL error: %s", (err ? err : "(null)"));
	delete mysql;
	return nullptr;
}

noinline static void test_select(MySQL *mysql)
{
	MySQLResult *res = nullptr;
	const char *err;
	MYSQL_ROW row;
	uint32_t i;

	if (mysql->RealQuery("SELECT 1 UNION SELECT 2 UNION SELECT 3;"))
		goto err;

	res = mysql->StoreResult();
	if (!res)
		goto err;

	i = 0;
	while (1) {
		row = res->FetchRow();
		if (unlikely(!row))
			goto err;
		if (IS_MYSQL_ROW_END(row))
			break;

		switch (++i) {
		case 1:
			assert(!strcmp(row[0], "1"));
			break;
		case 2:
			assert(!strcmp(row[0], "2"));
			break;
		case 3:
			assert(!strcmp(row[0], "3"));
			break;
		default:
			assert(0);
			break;
		}
	}

	delete res;
	return;
err:
	err = mysql->Error();
	pr_err("MySQL error: %s\n", (err ? err : "(null)"));
	delete res;
}


constexpr static const char q_test_insert__create_table[] =
"CREATE TABLE `aaa` ("
	"`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,"
	"`name` varchar(255) COLLATE 'utf8mb4_general_ci' NOT NULL,"
	"`nullable_string` varchar(255) NULL"
");";
static void test_insert__create_table(MySQL *mysql)
{
	int ret;

	ret = mysql->RealQuery("DROP TABLE IF EXISTS `aaa`;");
	assert(!ret);

	ret = mysql->RealQuery(q_test_insert__create_table);
	assert(!ret);
}

static void test_insert__insert_data(MySQL *mysql)
{
	const char *err;
	MySQLStmt *stmt;
	uint32_t buf_id;

	stmt = mysql->Prepare(9, "INSERT INTO `aaa` (id, name, nullable_string) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)");
	if (unlikely(!stmt)) {
		err = mysql->Error();
		pr_err("MySQL Error: %s", err);
		assert(0);
		return;
	}

	stmt->BindStr(0, "1");
	stmt->BindStr(1, "aaaaaaaaaaa");
	stmt->BindStr(2, "11111111111");

	stmt->BindStr(3, "2");
	stmt->BindStr(4, "bbbbbbbbbbb");
	stmt->BindStr(5, "22222222222");

	buf_id = 3;
	stmt->Bind(6, MYSQL_TYPE_LONGLONG, &buf_id, sizeof(buf_id));
	stmt->BindStr(7, "ccccccccccc");
	stmt->Bind(8, MYSQL_TYPE_NULL);

	if (unlikely(stmt->BindStmt()))
		goto err_stmt;
	if (unlikely(stmt->Execute()))
		goto err_stmt;

	delete stmt;
	return;

err_stmt:
	err = stmt->Error();
	pr_err("MySQLStmt Error: %s", err);
	delete stmt;
	assert(0);
	return;
}

static int test_insert__validate_insert_fetch(MySQLStmtResult *res)
{
	uint64_t i;
	uint64_t b_id;

	char b_name[256];
	size_t b_len_name;
	bool b_null_name;

	char b_nullable_string[256];
	size_t b_len_nullable_string;
	bool b_null_nullable_string;

	res->Bind(0, MYSQL_TYPE_LONGLONG, &b_id, sizeof(b_id));
	res->Bind(1, MYSQL_TYPE_STRING, &b_name, sizeof(b_name), &b_null_name,
		  &b_len_name);
	res->Bind(2, MYSQL_TYPE_STRING, &b_nullable_string,
		  sizeof(b_nullable_string), &b_null_nullable_string,
		  &b_len_nullable_string);
	res->BindResult();

	i = 0;
	while (1) {
		int ret = res->Fetch();
		if (unlikely(ret == 1))
			return 1;

		if (ret == MYSQL_NO_DATA)
			return 0;

		assert(ret != MYSQL_DATA_TRUNCATED);
		assert(ret == 0);

		i++;
		assert(i == b_id);
		switch (i) {
		case 1:
			assert(!strcmp(b_name, "aaaaaaaaaaa"));
			assert(!strcmp(b_nullable_string, "11111111111"));
			assert(!b_null_nullable_string);
			break;
		case 2:
			assert(!strcmp(b_name, "bbbbbbbbbbb"));
			assert(!strcmp(b_nullable_string, "22222222222"));
			assert(!b_null_nullable_string);
			break;
		case 3:
			assert(!strcmp(b_name, "ccccccccccc"));
			assert(b_null_nullable_string);
			break;
		}
	}
}

static void test_insert__validate_insert(MySQL *mysql)
{
	MySQLStmtResult *res;
	MySQLStmt *stmt;
	const char *err;

	stmt = mysql->Prepare(0, "SELECT * FROM `aaa`");
	if (unlikely(!stmt)) {
		err = mysql->Error();
		pr_err("MySQL Error: %s", err);
		assert(0);
		return;
	}

	if (unlikely(stmt->Execute()))
		goto err_stmt;

	res = stmt->StoreResult();
	if (unlikely(!res))
		goto err_stmt;

	if (unlikely(res->StoreResult()))
		goto err_stmt;

	if (unlikely(test_insert__validate_insert_fetch(res)))
		goto err_stmt;

	delete res;
	delete stmt;
	return;

err_stmt:
	err = stmt->Error();
	pr_err("MySQLStmt Error: %s", err);
	delete stmt;
	assert(0);
	return;
}

noinline static void test_insert(MySQL *mysql)
{
	test_insert__create_table(mysql);
	test_insert__insert_data(mysql);
	test_insert__validate_insert(mysql);
}

int main(void)
{
	MySQL *mysql = init_conn();

	if (!mysql)
		return 1;

	test_select(mysql);
	test_insert(mysql);
	delete mysql;
	return 0;
}
