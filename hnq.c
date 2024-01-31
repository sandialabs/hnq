/*
 * This software is provided for the limited purpose of responding to 
 * Sandia RFQ # 10274. This software is experimental and, as such, using it 
 * may result in anything from financial or data loss to loss of life. 
 * No warranties are expressed or implied. Use at your own risk.
 *
 * Lee Ward
 * Sandia National Laboratories, New Mexico
 * P.O. Box 5800
 * Albuquerque, NM 87185-1319
 *
 * lee@sandia.gov
 */

#define _GNU_SOURCE					/* elide DIRECT_IO */
#define _XOPEN_SOURCE 600
#define _LARGEFILE64_SOURCE
#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>
#include <math.h>
#include <float.h>
#include <limits.h>
#include <assert.h>
#include <time.h>
#include <libgen.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/time.h>
#include <time.h>

#include <sys/ioctl.h>
#if 0
#include <linux/fs.h>
#endif

#define MEMALIGN_IS_THREAD_SAFE	1

#define USEC_PER_SEC	(1000000)

static const char *myname;				/* invoked as */
static const char *optstr =				/* options string */
	"h"						/* usage */
	"d"						/* turn on debug */
#if defined(O_DIRECT)
	"x"						/* use direct-IO */
#endif
	"i:"						/* report interval */
	"s:"						/* size limit */
	;

static int debug = 0;					/* debug? */

static int interval = -1;				/* report interval */
static int allstop = 0;					/* threads stop? */

static void usage(void);
static void error(const char *fmt, ...);
static void syserror(const char *msg);
static void warn(const char *fmt, ...);
static void note(const char *fmt, ...);
static unsigned long long rate(unsigned long long, struct timeval *);

/*
 * Per-file information we keep.
 */
struct file_info {
	pthread_mutex_t mutex;				/* mutex */
	int	ncomplete;				/* completed threads */
	int	fd;					/* fildes */
	const char *name;				/* path */
	struct stat stat;				/* stat info */
	off_t	nxt;					/* next offset */
};

struct task {
	pthread_t thread;				/* thread ref */
	struct file_info *fi;				/* working file */
	size_t	bufsiz;					/* IO buffer size */
	off_t	(*nxtoff)(struct task *);		/* next IO */
	ssize_t	(*io)(int, void *, size_t, off_t);	/* IO func */
	const char *ioname;				/* IO func name */
	char	opname;					/* opname from args */
	unsigned long long opcount;			/* op count */
	struct timeval time;				/* time */
};

static pthread_mutex_t mutex =				/* global mutex */
    PTHREAD_MUTEX_INITIALIZER;
static pthread_key_t key;				/* per-thread globals */
static struct task *tasks = NULL;			/* tasks context */

static off_t seq(struct task *);
static off_t rnd(struct task *);

/*
 * Task ident from context.
 */
#define t_id(_t)	((long )((_t) - tasks))

static void *t_start(struct task *);

int
main(int argc, char *const argv[])
{
	int	flags;
	int	i;
	long long ll;
	int	fd;
	ssize_t	cc;
	unsigned seed;
	int	err;
	struct file_info *finfo, *fi;
	unsigned nfi;
	unsigned n, ntasks;
	struct task *t;
	char	*cp, *end;
	char	c;
	unsigned count;
	int	success;
	unsigned long ul;
	size_t	siz;
	int	m;
#if 0
	unsigned long long dksiz;
#endif

	myname = basename(argv[0]);
	flags = 0;
	ll = 0;
	while ((i = getopt(argc, argv, optstr)) != -1) {
		switch (i) {

		case 'h':				/* usage */
			usage();
			break;
		case 'd':				/* debug */
			debug = 1;
			break;
#if defined(O_DIRECT)
		case 'x':
			flags |= O_DIRECT;
			break;
#endif
		case 'i':
			interval = atoi(optarg);
			break;
		case 's':
			ll = strtoll(optarg, &end, 0);
			if ((*optarg != '\0' && *end != '\0')
			    || ll < 0 ||
			    (ll == LLONG_MAX && errno == ERANGE)) {
			    	error("Bad limit\n");
				usage();
			    }
			break;
		default:
			usage();
		}
	}

	if (!(argc - optind))
		usage();

	fd = open("/dev/urandom", O_RDONLY);
	if (fd < 0) {
		syserror("/dev/urandom");
		exit(1);
	}
	count = sizeof(seed);
	while (count) {
		cc = read(fd, (char *)&seed + sizeof(seed) - count, count);
		if (cc < 0) {
			syserror("/dev/urandom");
			exit(1);
		}
		count -= (size_t )cc;
	}
	note("Random seed is %u\n", seed);
	srandom(seed);

	if (pthread_key_create(&key, NULL) != 0) {
		syserror("Can't create per-thread context key");
		exit(1);
	}

	finfo = NULL;
	err = 1;					/* assume failure */
	do {
		nfi = argc - optind;
		finfo = malloc(nfi * sizeof(struct file_info));
		if (!finfo) {
			error("Can't allocate file info table: %s\n",
			      strerror(errno));
			break;
		}
		success = 1;
		for (count = 0, fi = finfo; count < nfi; count++, fi++) {
			fi->fd = -1;
			fi->name = NULL;
			if (pthread_mutex_init(&fi->mutex, NULL) != 0) {
				syserror("Can't initialize per-file mutex");
				nfi = count;
				break;
			}
			fi->nxt = 0;
		}
		if (!success)
			break;
		n = 16;
		tasks = malloc(n * sizeof(struct task));
		if (!tasks) {
			error("Can't allocate tasks table: %s\n",
			      strerror(errno));
			n = 0;
			break;
		}
		ntasks = 0;
		fi = finfo;
		success = 0;
		while (optind < argc) {
			cp = argv[optind];
			count = 0;
			m = 0;
			success = 0;
			for (;;) {
				c = 's';
				if (*cp != '\0' && *(cp + 1) == '@') {
					c = *cp;
					cp += 2;
				}
				ul = strtoul(cp, &end, 0);
				if (ntasks >= n) {
					n *= 2;
					tasks =
					    realloc(tasks,
						    n * sizeof(struct task));
					if (!tasks) {
						error(("Can't grow tasks table:"
						       " %s\n"),
						      strerror(errno));
						break;
					}
				}
				tasks[ntasks + count].fi = fi;
				tasks[ntasks + count].opname = c;
				if (c == 'r' || c == 's') {
					m |= 1;
					tasks[ntasks + count].io = pread;
					tasks[ntasks + count].ioname = "read";
				} else if (c == 'R' || c == 'S') {
					m |= 2;
					tasks[ntasks + count].io =
					    (ssize_t (*)(int,
							 void *,
							 size_t,
							 off_t))pwrite;
					tasks[ntasks + count].ioname = "write";
				} else {
					error("Bad directive in \"%s\".\n",
					      argv[optind]);
					break;
				}
				switch (c) {
				
				case 'r': case 'R':
					tasks[ntasks + count].nxtoff = rnd;
					break;
				case 's': case 'S':
					tasks[ntasks + count].nxtoff = seq;
					break;
				default:
					abort();	/* can't happen */
				}
				if (cp == end || *end == '\0') {
					if (!count)
						success = 1;
					tasks[ntasks + count].bufsiz = 0;
					count++;
					break;
				}
				siz = ul;
				if (ul == ULONG_MAX ||
				    (unsigned long )siz != ul) {
					/*
					 * Too big. ERANGE or not...
					 */
					error("Bad buffer size in \"%s\"\n",
					      argv[optind]);
					break;
				}
				tasks[ntasks + count].bufsiz = siz;
				count++;
				cp = end + 1;
				if (*end == ':') {
					success = 1;
					break;
				}
				if (*end != ',') {
					error ("Missing separator in \"%s\"\n",
					       argv[optind]);
					break;
				}
			}
			if (!success)
				break;
			optind++;
			ntasks += count;
			success = 0;
			fi->name = cp;
			switch (m) {
			case 1:
				m = O_RDONLY;
				break;
			case 2:
				m = O_WRONLY;
				break;
			case 3:
				m = O_RDWR;
				break;
			default:
				error("Unhandled case in switch\n");
				m = -1;
			}
			if (m < 0)
				break;
			fi->fd = open(fi->name, m | flags);
			if (fi->fd < 0) {
				error("Can't open \"%s\": %s\n",
				      fi->name,
				      strerror(errno));
				break;
			}
			if (fstat(fi->fd, &fi->stat) != 0) {
				error("Can't stat \"%s\": %s\n",
				      fi->name,
				      strerror(errno));
				break;
			}
#if 0
			if (S_ISBLK(fi->stat.st_mode)) {
				if (ioctl(fi->fd, BLKGETSIZE64, &dksiz) != 0) {
					error("Can't get size of \"%s\": %s\n",
					      fi->name,
					      strerror(errno));
					break;
				}
				fi->stat.st_size = dksiz;
				if (fi->stat.st_size < 0 ||
				    fi->stat.st_size != (off_t )dksiz) {
					error("Size %llu too big from \"%s\"\n",
					      dksiz,
					      fi->name);
				}
			}
#endif
			if (ll && (long long )fi->stat.st_size > ll)
				fi->stat.st_size = (off_t )ll;
			fi->ncomplete = 0;
			success = 1;
			fi++;
		}
		if (!success)
			break;
		err = 0;
	} while (0);

	if (!err) {
		do {
			success = 1;
			for (count = 0, t = tasks;
			     count < ntasks;
			     count++, t++) {
				siz = t->bufsiz;
				if (!siz)
					siz =
					    t->bufsiz =
					    t->fi->stat.st_blksize;
				siz += t->fi->stat.st_blksize - 1;
				if (siz < t->bufsiz)
					siz = t->bufsiz;
				siz /= t->fi->stat.st_blksize;
				siz *= t->fi->stat.st_blksize;
				if (siz != t->bufsiz) {
					warn(("%s: transfer length adjusted"
					      " from %zu to %zu.\n"),
					     t->fi->name,
					     t->bufsiz,
					     siz);
					t->bufsiz = siz;
				}
				err =
				    pthread_create(&t->thread,
						   NULL,
						   (void *(*)(void *))t_start,
						   t);
				if (err) {
					error("Can't create thread %u: %s\n",
					      t_id(t),
					      strerror(errno));
					ntasks = count;
					success = 0;
					break;
				}
			}
		} while (0);
		if (err) {
			for (count = ntasks, t = tasks; count; count--, t++)
				if (pthread_cancel(t->thread) != 0)
					error("Can't cancel thread %u: %s\n",
					      t_id(t),
					      strerror(errno));
		}
	} else
		ntasks = 0;

	for (count = 0; count < ntasks; count++) {
		t = &tasks[count];
		if (pthread_join(t->thread, (void **)&t) != 0) {
			error("Can't reap thread %u: %s\n",
			      count,
			      strerror(errno));
			err = 1;
		}
	}

	if (!err) {
		note("\n");
		for (count = 0; count < ntasks; count++) {
			t = &tasks[count];
			note("%c@%zu:%s ", t->opname, t->bufsiz, t->fi->name);
#if defined(O_DIRECT)
			if (flags & O_DIRECT)
				note("O_DIRECT ");
#endif
			note("%lld %lld.%lld (%lld)\n",
			     t->opcount,
			     (unsigned long long )t->time.tv_sec,
			     (unsigned long long )t->time.tv_usec,
			     rate(t->opcount, &t->time));
		}
	}

	if (tasks)
		free(tasks);
	if (finfo) {
		for (count = 0, fi = finfo; count < nfi; count++, fi++)
			if (fi->fd >= 0 && close(fi->fd) != 0)
				syserror(fi->name);
		free(finfo);
	}
	if (pthread_key_delete(key) != 0)
		syserror("Can't delete per-thread context key");

	return err;
}

/*
 * Print usage and exit with error.
 */
static void
usage()
{

	(void )fprintf(stderr,
		       ("Usage: %s"
		        " [-%s] <[[[sS][rR]@]xfrlen[,...]:]path> ...\n"),
		       myname,
		       optstr);
	exit(1);
}

/*
 * Print error message from va_list.
 */
static void
verror(const char *fmt, va_list ap)
{

	vfprintf(stderr, fmt, ap);
	(void )fflush(stderr);
}

/*
 * Print error message.
 */
static void
error(const char *fmt, ...)
{
	va_list	ap;

	va_start(ap, fmt);
	verror(fmt, ap);
	va_end(ap);
}

/*
 * Print note from va_list.
 */
static void
vnote(const char *fmt, va_list ap)
{

	vfprintf(stdout, fmt, ap);
	(void )fflush(stdout);
}

/*
 * Print note.
 */
static void
note(const char *fmt, ...)
{
	va_list	ap;

	va_start(ap, fmt);
	vnote(fmt, ap);
	va_end(ap);
}

/*
 * Print system error message in <msg>:<error string> format.
 */
static void
syserror(const char *msg)
{

	error("%s: %s\n", msg, strerror(errno));
}

#if !defined(_GNU_SOURCE)
/*
 * Print a string to allocated memory from va_list.
 */
static int
vasprintf(char **strp, const char *fmt, va_list ap)
{
	size_t  siz;
	int     oerrno;
	char    *s;
	va_list aq;
	int     n;

	siz = 50;
	oerrno = errno;
	if (!(s = malloc(siz))) {
		errno = oerrno;
		return -1;
	}
	for (;;) {
		va_copy(aq, ap);
		n = vsnprintf (s, siz, fmt, aq);
		va_end(aq);
		if (n > -1 && (size_t )n < siz)
			break;
		if (n > -1)					/* glibc 2.1 */
			siz = n+1;
		else						/* glibc 2.0 */
			siz *= 2;
		if (!(s = realloc (s, siz)))
			break;
		}
	*strp = s;
	errno = oerrno;
	return n;
}

/*
 * Print a string to allocated memory.
 */
static int
asprintf(char **strp, const char *fmt, ...)
{
	va_list	ap;
	int	rtn;

	va_start(ap, fmt);
	rtn = vasprintf(strp, fmt, ap);
	va_end(ap);
	return rtn;
}
#endif /* !defined(_GNU_SOURCE) */

/*
 * Print warning message.
 */
static void
warn(const char *fmt, ...)
{
	char	*cp;
	va_list	ap;

	if (asprintf(&cp, "(warning) %s", fmt) < 0) {
		error("Can't allocate memory for warning string leader\n");
		return;
	}
	va_start(ap, fmt);
	verror(cp, ap);
	va_end(ap);
	free(cp);
}

/*
 * Print note from thread using va_list.
 */
static void
t_vnote(const char *fmt, va_list ap)
{
	struct task *t;
	char	*cp;

	t = pthread_getspecific(key);
	if (t == NULL) {
		error("Context not set in thread\n");
		return;
	}
	if (asprintf(&cp, "[%ld] %s", t_id(t), fmt) < 0) {
		error("Can't allocate memory for string leader\n");
		return;
	}
	vnote(cp, ap);
	free(cp);
}

/*
 * Print error message from thread using va_list.
 */
static void
t_verror(const char *fmt, va_list ap)
{
	struct task *t;
	char	*cp;

	t = pthread_getspecific(key);
	if (t == NULL) {
		error("Context not set in thread\n");
		return;
	}
	if (asprintf(&cp, "[%ld] %s", t_id(t), fmt) < 0) {
		error("Can't allocate memory for error string leader\n");
		return;
	}
	verror(cp, ap);
	free(cp);
}

/*
 * Print note from thread.
 */
static void
t_note(const char *fmt, ...)
{
	va_list	ap;

	va_start(ap, fmt);
	t_vnote(fmt, ap);
	va_end(ap);
}

/*
 * Print error message from thread.
 */
static void
t_error(const char *fmt, ...)
{
	va_list	ap;

	va_start(ap, fmt);
	t_verror(fmt, ap);
	va_end(ap);
}

/*
 * Print system error message from thread.
 */
static void
t_syserror(const char *msg)
{

	t_error("%s: %s\n", msg, strerror(errno));
}

/*
 * Print debug message from thread.
 */
static void
t_dbg(const char *fmt, ...)
{
	char	*cp;
	va_list	ap;

	if (asprintf(&cp, "(debug) %s", fmt) < 0) {
		error("Can't allocate memory for debug string leader\n");
		return;
	}
	va_start(ap, fmt);
	t_verror(cp, ap);
	va_end(ap);
	free(cp);
}

static off_t
seq(struct task *t)
{
	off_t	off;
	size_t	count;

	if (pthread_mutex_lock(&t->fi->mutex) != 0) {
		t_syserror("Can't lock mutex");
		return -1;
	}
	do {
		off = t->fi->nxt;
		count = t->fi->stat.st_size - (size_t )off;
		if (!count) {
			off = -1;
			break;
		}
		if (count > t->bufsiz)
			count = t->bufsiz;
		t->fi->nxt += count;
	} while (0);
	if (pthread_mutex_unlock(&t->fi->mutex) != 0) {
		t_syserror("Can't unlock mutex");
		abort();
	}
	return off;
}

/*
 * Deliver uniform random number in [0,hi].
 *
 * NB: *Not* reentrant.
 */
static double
drnd(double hi)
{

	return random() * (hi / RAND_MAX);
}

static off_t
rnd_helper(struct task *t, double (*f)(double))
{
	int	eof;
	off_t	off;

	eof = 0;
	if (pthread_mutex_lock(&t->fi->mutex) != 0) {
		t_syserror("Can't lock mutex");
		return -1;
	}
	do {
		if (t->fi->ncomplete) {
			eof = 1;
			break;
		}
		off = t->fi->stat.st_size;
	} while (0);
	if (pthread_mutex_unlock(&t->fi->mutex) != 0) {
		t_syserror("Can't unlock mutex");
		abort();
	}
	off /= t->bufsiz;
	off -= 1;
	if (pthread_mutex_lock(&mutex) != 0) {
		t_syserror("Can't lock global mutex");
		return -1;
	}
	off = (*f)(off);
	if (pthread_mutex_unlock(&mutex) != 0) {
		t_syserror("Can't unlock global mutex");
		abort();
	}
	if (eof)
		return -1;
	off *= t->bufsiz;
	return off;
}

static off_t
rnd(struct task *t)
{

	return rnd_helper(t, drnd);
}

static void
wallclock(struct timeval *tvp)
{

	if (gettimeofday(tvp, NULL) != 0)
		abort();
}

static unsigned long long
rate(unsigned long long nops, struct timeval *tvp)
{
	unsigned long long n;

	n = tvp->tv_sec ? nops / tvp->tv_sec : 0;
	if (tvp->tv_usec > (USEC_PER_SEC / 2))
		n++;
	return n;
}

static void *
t_start(struct task *t)
{
	struct timeval rtime, now, elapsed;
	int	success;
	char	*buf;
	struct file_info *fi;
	int	err;
	off_t	off;
	size_t	count;
	ssize_t	cc;

	if (pthread_setspecific(key, t) != 0) {
		error("Thread %u cannot set context: %s\n",
		      t_id(t),
		      strerror(errno));
		return NULL;
	}
	wallclock(&t->time);
	rtime = t->time;
	if (debug)
		t_dbg("Thread start: %llu.%llu\n",
		      (unsigned long long )t->time.tv_sec,
		      (unsigned long long )t->time.tv_usec);
	t->opcount  = 0;
	success = 0;
	do {
		fi = t->fi;
#if defined(MEMALIGN_IS_THREAD_SAFE) && !MEMALIGN_IS_THREAD_SAFE
		if (pthread_mutex_lock(&mutex) != 0) {
			t_syserror("Can't lock global mutex");
			break;
		}
#endif
		err =
		    posix_memalign((void **)&buf,
				   fi->stat.st_blksize,
				   t->bufsiz);
#if defined(MEMALIGN_IS_THREAD_SAFE) && !MEMALIGN_IS_THREAD_SAFE
		if (pthread_mutex_unlock(&mutex) != 0) {
			t_syserror("Can't unlock global mutex");
			abort();
		}
#endif
		if (err) {
			buf = NULL;
			syserror("Can't allocate IO buffer");
			break;
		}
		while (!allstop && (off = (*t->nxtoff)(t)) >= 0) {
			count = fi->stat.st_size - off;
			if (count > t->bufsiz)
				count = t->bufsiz;
			if (debug)
				t_dbg("%s: %s %zu@%lld\n",
				      fi->name,
				      t->ioname,
				      count,
				      (long long )off);
			cc = (*t->io)(fi->fd, buf, count, off);
			if (cc < 0)
				break;
			if ((size_t )cc != count) {
				t_error("%s: short %s (%zd of %zu)\n",
					fi->name,
					t->ioname,
					cc,
					count);
				break;
			}
			t->opcount++;
			if (interval > 0) {
				wallclock(&now);
				timersub(&now, &rtime, &elapsed);
				if (elapsed.tv_sec >= interval) {
					timersub(&now, &t->time, &elapsed);
					t_note(("%c@%zu:%s %"
					        "lld %lld.%lld (%lld)\n"),
					       t->opname,
					       t->bufsiz,
					       t->fi->name,
					       t->opcount,
					       (unsigned long long )elapsed.tv_sec,
					       (unsigned long long )elapsed.tv_usec,
					       rate(t->opcount,
						    &elapsed));
					rtime = now;
				}
			}
		}
		if (cc < 0 || (size_t )cc != count)
			break;
		success = 1;
	} while (0);
	wallclock(&now);
	timersub(&now, &t->time, &elapsed);
	t->time = elapsed;
	if (buf)
		free(buf);
	if (debug)
		t_dbg("Thread returns %s: %llu.%llu\n",
		      success ? "true" : "false",
		      (unsigned long long )t->time.tv_sec,
		      (unsigned long long )t->time.tv_usec);
	allstop = 1;
	return success? t : NULL;
}
