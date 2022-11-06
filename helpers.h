
#ifndef CPPMYSQL__HELPERS_H
#define CPPMYSQL__HELPERS_H

#ifndef likely
#define likely(COND) __builtin_expect(!!(COND), 1)
#endif

#ifndef unlikely
#define unlikely(COND) __builtin_expect(!!(COND), 0)
#endif

#ifndef noinline
#define noinline __attribute__((__noinline__))
#endif

#ifndef __cold
#define __cold __attribute__((__cold__))
#endif

#ifndef __hot
#define __hot __attribute__((__hot__))
#endif

#endif /* #ifndef CPPMYSQL__HELPERS_H */
