
#ifndef CPP_MYSQL_H
#define CPP_MYSQL_H

#include <mysql/mysql.h>

#include <new>
#include <cstdio>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <cassert>

#ifndef likely
#define likely(COND) __builtin_expect(!!(COND), 1)
#endif

#ifndef unlikely
#define unlikely(COND) __builtin_expect(!!(COND), 0)
#endif

#ifndef noinline
#define noinline __attribute__((__noinline__))
#endif

namespace CppMySQL {

#define SetErr(...)					\
do {							\
	snprintf(err_, sizeof(err_), __VA_ARGS__);	\
} while (0)

#define SetMySQLErrFallback(...)			\
do {							\
	const char *___err = mysql_error(conn_);	\
							\
	if (___err && ___err[0])			\
		SetErr("%s", ___err);			\
	else						\
		SetErr(__VA_ARGS__);			\
} while (0)


class MySQLStmt;
class MySQLResult;

class MySQL {
public:
	inline MySQL(const char *host, const char *user = nullptr,
		     const char *passwd = nullptr, const char *dbname = nullptr,
		     uint32_t port = 0) noexcept:
		host_(host),
		user_(user),
		passwd_(passwd),
		dbname_(dbname),
		port_(port)
	{
		ClearErr();
	}

	~MySQL(void) noexcept;
	int Init(void) noexcept;
	int Connect(void) noexcept;

	MySQLStmt *Prepare(size_t nr_bind, const char *q, size_t qlen) noexcept;

	inline MySQLStmt *Prepare(size_t nr_bind, const char *q) noexcept
	{
		return Prepare(nr_bind, q, strlen(q));
	}

	inline unsigned int Errno(void) noexcept
	{
		return mysql_errno(conn_);
	}

	const char *Error(void) noexcept;

	inline int RealQuery(const char *q, unsigned long len) noexcept
	{	
		return mysql_real_query(conn_, q, len);
	}

	inline int RealQuery(const char *q) noexcept
	{	
		return mysql_real_query(conn_, q, strlen(q));
	}

	MySQLResult *StoreResult(void) noexcept;

	inline void Close(void) noexcept
	{
		mysql_close(conn_);
		conn_ = nullptr;
	}

	inline MYSQL *Conn(void) noexcept
	{
		return conn_;
	}

	inline void ClearErr(void) noexcept
	{
		err_[0] = '\0';
	}

private:
	MYSQL		*conn_ = nullptr;
	const char	*host_;
	const char	*user_;
	const char	*passwd_;
	const char	*dbname_;
	uint16_t	port_ = 0;
	char		err_[128];
};


#define MYSQL_ROW_END_PTR (reinterpret_cast<MYSQL_ROW>(-1ul))
#define IS_MYSQL_ROW_END(ARG) ((ARG) == MYSQL_ROW_END_PTR)

class MySQL;

class MySQLResult {
public:
	inline MySQLResult(MySQL *mysql, MYSQL_RES *res) noexcept:
		res_(res),
		mysql_(mysql)
	{
	}

	inline ~MySQLResult(void) noexcept
	{
		mysql_free_result(res_);
	}

	inline uint64_t NumRows(void) noexcept
	{
		return mysql_num_rows(res_);
	}

	MYSQL_ROW FetchRow(void) noexcept;

private:
	MYSQL_RES	*res_;
	MySQL		*mysql_;
};

class MySQLStmtResult {
public:
	inline MySQLStmtResult(MYSQL_STMT *stmt, MYSQL_RES *res,
			       MYSQL_BIND *bind) noexcept:
		res_(res),
		stmt_(stmt),
		bind_(bind)
	{
	}

	inline MYSQL_BIND *Bind(size_t i)
	{
		return &bind_[i];
	}

	inline MYSQL_BIND *Bind(size_t i, enum enum_field_types type, void *buf,
				size_t buflen, bool *is_null = nullptr,
				size_t *len = nullptr) noexcept
	{
		MYSQL_BIND *b = &bind_[i];

		b->buffer_type = type;
		b->buffer = buf;
		b->buffer_length = buflen;
		b->is_null = is_null;
		b->length = len;
		return b;
	}

	inline int StoreResult(void) noexcept
	{
		return mysql_stmt_store_result(stmt_);
	}

	inline int BindResult(void) noexcept
	{
		return mysql_stmt_bind_result(stmt_, bind_);
	}

	inline size_t NumFields(void) noexcept
	{
		return mysql_num_fields(res_);
	}

	inline int Fetch(void) noexcept
	{
		return mysql_stmt_fetch(stmt_);
	}

	inline ~MySQLStmtResult(void) noexcept
	{
		free(bind_);
		mysql_free_result(res_);
	}

private:
	MYSQL_RES	*res_;
	MYSQL_STMT	*stmt_;
	MYSQL_BIND	*bind_;
};

class MySQLStmt {
public:
	inline MySQLStmt(MySQL *mysql, MYSQL_STMT *stmt):
		stmt_(stmt),
		bind_(nullptr),
		mysql_(mysql)
	{
		ClearErr();
	}

	inline MySQLStmt(MySQL *mysql, MYSQL_STMT *stmt, MYSQL_BIND *bind):
		stmt_(stmt),
		bind_(bind),
		mysql_(mysql)
	{
		ClearErr();
	}

	inline void ClearErr(void) noexcept
	{
		err_[0] = '\0';
	}

	inline MYSQL_BIND *Bind(size_t i)
	{
		return &bind_[i];
	}

	inline ~MySQLStmt(void) noexcept
	{
		mysql_stmt_close(stmt_);
		if (bind_)
			free(bind_);
	}

	inline MYSQL_BIND *BindStr(size_t i, const char *str, size_t len)
		noexcept
	{
		MYSQL_BIND *b = &bind_[i];

		b->buffer_type = MYSQL_TYPE_STRING;
		b->buffer = const_cast<void *>(static_cast<const void *>(str));
		b->buffer_length = len;
		return b;
	}

	inline MYSQL_BIND *BindStr(size_t i, const char *str) noexcept
	{
		return BindStr(i, str, strlen(str));
	}

	inline MYSQL_BIND *Bind(size_t i, enum enum_field_types type,
				void *buf = nullptr, size_t buflen = 0) noexcept
	{
		MYSQL_BIND *b = &bind_[i];

		b->buffer_type = type;
		b->buffer = buf;
		b->buffer_length = buflen;
		return b;
	}

	inline int BindStmt(void) noexcept
	{
		return mysql_stmt_bind_param(stmt_, bind_);
	}

	inline int Execute(void) noexcept
	{
		return mysql_stmt_execute(stmt_);
	}

	inline unsigned int Errno(void) noexcept
	{
		return mysql_stmt_errno(stmt_);
	}

	const char *Error(void) noexcept;

	inline unsigned int MySQLErrno(void) noexcept
	{
		return mysql_errno(mysql_->Conn());
	}

	inline const char *MySQLError(void) noexcept
	{
		return mysql_error(mysql_->Conn());
	}

	MySQLStmtResult *StoreResult(void) noexcept;

private:
	MYSQL_STMT	*stmt_;
	MYSQL_BIND	*bind_;
	MySQL		*mysql_;
	char		err_[128];
};


#if !defined(KEEP_CPPMYSQL_HELPER_MACROS)
#undef SetErr
#undef SetMySQLErrFallback
#endif

} /* namespace CppMySQL */

#endif /* #ifndef CPP_MYSQL_H */
