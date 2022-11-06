
#define KEEP_CPPMYSQL_HELPER_MACROS
#include "MySQL.h"
#include "helpers.h"

namespace CppMySQL {

__cold
MySQL::~MySQL(void) noexcept
{
	if (conn_)
		Close();
}

__cold
int MySQL::Init(void) noexcept
{
	conn_ = mysql_init(NULL);
	if (!conn_) {
		SetErr("Failed to perform mysql_init()");
		return -ENOMEM;
	}

	return 0;
}

__cold
int MySQL::Connect(void) noexcept
{
	MYSQL *ret;

	ret = mysql_real_connect(conn_, host_, user_, passwd_, dbname_, port_,
				 NULL, 0);
	if (unlikely(!ret)) {
		SetMySQLErrFallback("Failed to perform mysql_real_connect()");
		return -1;
	}

	assert(ret == conn_);
	return 0;
}

__hot
const char *MySQL::Error(void) noexcept
{
	if (err_[0])
		return err_;

	if (conn_) {
		const char *ret = mysql_error(conn_);
		if (ret && ret[0])
			return ret;
	}

	return nullptr;
}

__hot
MySQLResult *MySQL::StoreResult(void) noexcept
{
	MySQLResult *ret;
	MYSQL_RES *res;

	res = mysql_store_result(conn_);
	if (unlikely(!res)) {
		ClearErr();
		return nullptr;
	}

	ret = new(std::nothrow) MySQLResult(this, res);
	if (unlikely(!ret)) {
		mysql_free_result(res);
		SetErr("Cannot allocate memory for MySQLResult");
		return nullptr;
	}

	return ret;
}

__hot
MYSQL_ROW MySQLResult::FetchRow(void) noexcept
{
	MYSQL_ROW ret;

	ret = mysql_fetch_row(res_);
	if (unlikely(!ret)) {
		mysql_->ClearErr();
		if (unlikely(mysql_->Error()))
			return nullptr;

		return MYSQL_ROW_END_PTR;
	}

	return ret;
}

__hot
MySQLStmt *MySQL::Prepare(size_t nr_bind, const char *q, size_t qlen) noexcept
{
	MYSQL_BIND *bind = nullptr;
	MYSQL_STMT *stmt;
	MySQLStmt *ret;
	int tmp;

	stmt = mysql_stmt_init(conn_);
	if (unlikely(!stmt))
		return nullptr;

	tmp = mysql_stmt_prepare(stmt, q, qlen);
	if (unlikely(tmp)) {
		const char *err = mysql_stmt_error(stmt);

		if (err && *err)
			SetErr("%s", err);
		goto err;
	}

	if (nr_bind > 0) {
		bind = static_cast<MYSQL_BIND *>(calloc(nr_bind, sizeof(*bind)));
		if (unlikely(!bind)) {
			SetErr("Cannot allocate memory for MYSQL_BIND");
			goto err;
		}
	}

	ret = new(std::nothrow) MySQLStmt(this, stmt, bind);
	if (unlikely(!ret)) {
		SetErr("Cannot allocate memory for MySQLStmt");
		goto err;
	}

	return ret;

err:
	free(bind);
	mysql_stmt_close(stmt);
	return nullptr;
}

__hot
const char *MySQLStmt::Error(void) noexcept
{
	if (err_[0])
		return err_;

	if (stmt_) {
		const char *ret = mysql_stmt_error(stmt_);
		if (ret && ret[0])
			return ret;
	}

	return nullptr;
}

MySQLStmtResult *MySQLStmt::StoreResult(void) noexcept
{
	MySQLStmtResult *ret = nullptr;
	MYSQL_BIND *bind = nullptr;
	MYSQL_RES *res;
	size_t nfields;

	res = mysql_stmt_result_metadata(stmt_);
	if (unlikely(!res))
		return nullptr;

	nfields = mysql_num_fields(res);
	bind = static_cast<MYSQL_BIND *>(calloc(nfields, sizeof(*bind)));
	if (unlikely(!bind)) {
		SetErr("Cannot allocate memory for MYSQL_BIND");
		goto err;
	}

	ret = new(std::nothrow) MySQLStmtResult(stmt_, res, bind);
	if (unlikely(!ret)) {
		SetErr("Cannot allocate memory for MySQLStmtResult");
		goto err;
	}

	return ret;

err:
	free(bind);
	mysql_free_result(res);
	return nullptr;
}

} /* namespace CppMySQL */
