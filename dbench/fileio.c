/* 
   dbench version 4

   Copyright (C) 1999-2007 by Andrew Tridgell <tridge@samba.org>
   Copyright (C) 2001 by Martin Pool <mbp@samba.org>
   
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 3 of the License, or
   (at your option) any later version.
   
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   
   You should have received a copy of the GNU General Public License
   along with this program; if not, see <http://www.gnu.org/licenses/>.
*/

#include "dbench.h"

#define MAX_FILES 200

struct ftable {
	char *name;
	int fd;
	int handle;
};

static int find_handle(struct child_struct *child, int handle)
{
	struct ftable *ftable = child->private;
	int i;
	for (i=0;i<MAX_FILES;i++) {
		if (ftable[i].handle == handle) return i;
	}
	printf("(%d) ERROR: handle %d was not found\n", 
	       child->line, handle);
	exit(1);
}


/* Find the directory holding a file, and flush it to disk.  We do
   this in -S mode after a directory-modifying mode, to simulate the
   way knfsd tries to flush directories.  MKDIR and similar operations
   are meant to be synchronous on NFSv2. */
static void sync_parent(struct child_struct *child, const char *fname)
{
	char copy_name[64];
	int dir_fd;
	char *slash;

	if (strchr(fname, '/')) {
		strncpy(copy_name, fname, sizeof(copy_name));
		slash = strrchr(copy_name, '/');
		*slash = '\0';
	} else {
		strncpy(copy_name, ".", sizeof(copy_name));
	}

	dir_fd = open(copy_name, O_RDONLY);
	if (dir_fd == -1) {
		printf("[%d] open directory \"%s\" for sync failed\n",
		       child->line, copy_name);
	} else {
		if (fsync(dir_fd) == -1) {
			printf("[%d] datasync directory \"%s\" failed\n",
			       child->line, copy_name);
		}
		if (close(dir_fd) == -1) {
			printf("[%d] close directory failed\n",
			       child->line);
		}
	}
}

static int expected_status(const char *status)
{
	if (strcmp(status, "NT_STATUS_OK") == 0) {
		return 0;
	}
	if (strncmp(status, "0x", 2) == 0 &&
	    strtoul(status, NULL, 16) == 0) {
		return 0;
	}
	return -1;
}

/*
  simulate pvfs_resolve_name()
*/
static void resolve_name(struct child_struct *child, const char *name)
{
	struct stat st;
	char dname[64], *fname;
#ifdef XV6_USER
	int dirfd;
	char *prevname = NULL;
	char namebuf[DIRSIZ+1];
#else
	DIR *dir;
	struct dirent *d;
#endif
	char *p;

	if (name == NULL) return;

	if (stat(name, &st) == 0)
		return;

	if (options.no_resolve) {
		return;
	}

	strncpy(dname, name, sizeof(dname));
	p = strrchr(dname, '/');
	if (!p) return;
	*p = 0;
	fname = p+1;

#ifdef XV6_USER
	dirfd = open(dname, O_RDONLY|O_DIRECTORY);
	if (dirfd < 0)
		return;
	while (readdir(dirfd, prevname, namebuf) > 0) {
		prevname = namebuf;
		if (strcasecmp(fname, namebuf) == 0)
			break;
	}
	close(dirfd);
#else
	dir = opendir(dname);
	if (!dir)
		return;
	while ((d = readdir(dir))) {
		if (strcasecmp(fname, d->d_name) == 0) break;
	}
	closedir(dir);
#endif
}

static void failed(struct child_struct *child)
{
	child->failed = 1;
	printf("ERROR: child %d failed at line %d\n", child->id, child->line);
	exit(1);
}

static void fio_setup(struct child_struct *child)
{
	struct ftable *ftable;
	ftable = calloc(MAX_FILES, sizeof(struct ftable));
	child->private = ftable;
	child->rate.last_time = timeval_current();
	child->rate.last_bytes = 0;
}

static void fio_unlink(struct dbench_op *op)
{
	resolve_name(op->child, op->fname);

	if (unlink(op->fname) != expected_status(op->status)) {
		printf("[%d] unlink %s failed - expected %s\n", 
		       op->child->line, op->fname, op->status);
		failed(op->child);
	}
	if (options.sync_dirs) sync_parent(op->child, op->fname);
}

static void fio_mkdir(struct dbench_op *op)
{
	struct stat st;
	resolve_name(op->child, op->fname);
	if (options.stat_check && stat(op->fname, &st) == 0) {
		return;
	}
	mkdir(op->fname, 0777);
}

static void fio_rmdir(struct dbench_op *op)
{
	struct stat st;
	resolve_name(op->child, op->fname);

	if (options.stat_check && 
	    (stat(op->fname, &st) != 0 || !S_ISDIR(st.st_mode))) {
		return;
	}

	if (rmdir(op->fname) != expected_status(op->status)) {
		printf("[%d] rmdir %s failed - expected %s\n", 
		       op->child->line, op->fname, op->status);
		failed(op->child);
	}
	if (options.sync_dirs) sync_parent(op->child, op->fname);
}

static void fio_createx(struct dbench_op *op)
{
	uint32_t create_options = op->params[0];
	uint32_t create_disposition = op->params[1];
	int fnum = op->params[2];
	int fd, i;
	int flags = O_RDWR;
	struct stat st;
	struct ftable *ftable = (struct ftable *)op->child->private;

	resolve_name(op->child, op->fname);

	if (create_disposition == FILE_CREATE) {
		if (options.stat_check && stat(op->fname, &st) == 0) {
			create_disposition = FILE_OPEN;
		} else {
			flags |= O_CREAT;
		}
	}

	if (create_disposition == FILE_OVERWRITE ||
	    create_disposition == FILE_OVERWRITE_IF) {
		flags |= O_CREAT | O_TRUNC;
	}

	if (create_options & FILE_DIRECTORY_FILE) {
		/* not strictly correct, but close enough */
		if (!options.stat_check || stat(op->fname, &st) == -1) {
			mkdir(op->fname, 0700);
		}
	}

	if (create_options & FILE_DIRECTORY_FILE) flags = O_RDONLY|O_DIRECTORY;

	fd = open(op->fname, flags, 0600);
	if (fd < 0) {
		flags = O_RDONLY|O_DIRECTORY;
		fd = open(op->fname, flags, 0600);
	}
	if (fd == -1) {
		if (expected_status(op->status) == 0) {
			printf("[%d] open %s failed for handle %d\n", 
			       op->child->line, op->fname, fnum);
		}
		return;
	}
	if (expected_status(op->status) != 0) {
		printf("[%d] open %s succeeded for handle %d\n", 
		       op->child->line, op->fname, fnum);
		close(fd);
		return;
	}
	
	for (i=0;i<MAX_FILES;i++) {
		if (ftable[i].handle == 0) break;
	}
	if (i == MAX_FILES) {
		printf("file table full for %s\n", op->fname);
		exit(1);
	}

	ftable[i].name = (char *)malloc(64);
	strncpy(ftable[i].name, op->fname, 64);
	ftable[i].handle = fnum;
	ftable[i].fd = fd;

	fstat(fd, &st);
}

static void fio_writex(struct dbench_op *op)
{
	int handle = op->params[0];
	int offset = op->params[1];
	int size = op->params[2];
	int ret_size = op->params[3];
	int i = find_handle(op->child, handle);
	void *buf;
	struct stat st;
	struct ftable *ftable = (struct ftable *)op->child->private;
	ssize_t ret;

	if (options.fake_io) {
		op->child->bytes += ret_size;
		op->child->bytes_since_fsync += ret_size;
		return;
	}

	buf = calloc(size, 1);

	if (options.one_byte_write_fix &&
	    size == 1 && fstat(ftable[i].fd, &st) == 0) {
		if (st.st_size > offset) {
			unsigned char c;
			pread(ftable[i].fd, &c, 1, offset);
			if (c == ((unsigned char *)buf)[0]) {
				free(buf);
				op->child->bytes += size;
				return;
			}
		}
	}

	ret = pwrite(ftable[i].fd, buf, size, offset);
	if (ret == -1) {
		printf("[%d] write failed on handle %d\n", 
		       op->child->line, handle);
		exit(1);
	}
	if (ret != ret_size) {
		printf("[%d] wrote %d bytes, expected to write %d bytes on handle %d\n", 
		       op->child->line, (int)ret, (int)ret_size, handle);
		exit(1);
	}

	if (options.do_fsync) fsync(ftable[i].fd);

	free(buf);

	op->child->bytes += size;
	op->child->bytes_since_fsync += size;
}

static void fio_readx(struct dbench_op *op)
{
	int handle = op->params[0];
	int offset = op->params[1];
	int size = op->params[2];
	int ret_size = op->params[3];
	int i = find_handle(op->child, handle);
	void *buf;
	struct ftable *ftable = (struct ftable *)op->child->private;

	if (options.fake_io) {
		op->child->bytes += ret_size;
		return;
	}

	buf = malloc(size);

	if (pread(ftable[i].fd, buf, size, offset) != ret_size) {
		printf("[%d] read failed on handle %d\n", 
		       op->child->line, handle);
	}

	free(buf);

	op->child->bytes += size;
}

static void fio_close(struct dbench_op *op)
{
	int handle = op->params[0];
	struct ftable *ftable = (struct ftable *)op->child->private;
	int i = find_handle(op->child, handle);
	close(ftable[i].fd);
	ftable[i].handle = 0;
	if (ftable[i].name) free(ftable[i].name);
	ftable[i].name = NULL;
}

static void fio_rename(struct dbench_op *op)
{
	const char *old = op->fname;
	const char *new = op->fname2;

	resolve_name(op->child, old);
	resolve_name(op->child, new);

	if (options.stat_check) {
		struct stat st;
		if (stat(old, &st) != 0 && expected_status(op->status) == 0) {
			printf("[%d] rename %s %s failed - file doesn't exist\n",
			       op->child->line, old, new);
			failed(op->child);
			return;
		}
	}

	if (rename(old, new) != expected_status(op->status)) {
		printf("[%d] rename %s %s failed - expected %s\n", 
		       op->child->line, old, new, op->status);
		failed(op->child);
	}
	if (options.sync_dirs) sync_parent(op->child, new);
}

static void fio_flush(struct dbench_op *op)
{
	int handle = op->params[0];
	struct ftable *ftable = (struct ftable *)op->child->private;
	int i = find_handle(op->child, handle);
	fsync(ftable[i].fd);
}

static void fio_qpathinfo(struct dbench_op *op)
{
	resolve_name(op->child, op->fname);
}

static void fio_qfileinfo(struct dbench_op *op)
{
	int handle = op->params[0];
	int level = op->params[1];
	struct ftable *ftable = (struct ftable *)op->child->private;
	struct stat st;
	int i = find_handle(op->child, handle);
	(void)op->child;
	(void)level;
	fstat(ftable[i].fd, &st);
}

static void fio_findfirst(struct dbench_op *op)
{
	int level = op->params[0];
	int maxcnt = op->params[1];
	int count = op->params[2];
#ifdef XV6_USER
	int dirfd;
	char *prevname = NULL;
	char namebuf[DIRSIZ+1];
#else
	DIR *dir;
	struct dirent *d;
#endif
	char *p;

	(void)op->child;
	(void)level;
	(void)count;

	resolve_name(op->child, op->fname);

#ifdef XV6_USER
	if (strchr(op->fname, '<') == NULL &&
	    strchr(op->fname, '>') == NULL &&
	    strchr(op->fname, '*') == NULL &&
	    strchr(op->fname, '?') == NULL &&
	    strchr(op->fname, '"') == NULL)
		return;
#else
	if (strpbrk(op->fname, "<>*?\"") == NULL) {
		return;
	}
#endif

	p = strrchr(op->fname, '/');
	if (!p) return;
	*p = 0;
#ifdef XV6_USER
	dirfd = open(op->fname, O_RDONLY|O_DIRECTORY);
	if (dirfd < 0)
		return;
	while (maxcnt && (readdir(dirfd, prevname, namebuf) > 0)) {
		prevname = namebuf;
		maxcnt--;
	}
	close(dirfd);
#else
	dir = opendir(op->fname);
	if (!dir) return;
	while (maxcnt && (d = readdir(dir))) maxcnt--;
	closedir(dir);
#endif
}

static void fio_deltree(struct dbench_op *op)
{
#ifdef XV6_USER
	int dirfd;
	char *prevname = NULL;
	char namebuf[DIRSIZ+1];

	dirfd = open(op->fname, O_RDONLY|O_DIRECTORY);
	if (dirfd < 0)
		return;

	while (readdir(dirfd, prevname, namebuf) > 0) {
		prevname = namebuf;

		struct stat st;
		char *fname = (char *)malloc(64);
		if (strcmp(namebuf, ".") == 0 ||
		    strcmp(namebuf, "..") == 0) {
			continue;
		}
		snprintf(fname, 64, "%s/%s", op->fname, namebuf);
		if (fname == NULL) {
			printf("Out of memory\n");
			exit(1);
		}
		if (stat(fname, &st) != 0) {
			continue;
		}
		if (S_ISDIR(st.st_mode)) {
			struct dbench_op op2 = *op;
			op2.fname = fname;
			fio_deltree(&op2);
		} else {
			if (unlink(fname) != 0) {
				printf("[%d] unlink '%s' failed\n",
				       op->child->line, fname);
			}
		}
		free(fname);
	}
	close(dirfd);
#else
	DIR *d;
	struct dirent *de;

	d = opendir(op->fname);
	if (d == NULL) return;

	for (de=readdir(d);de;de=readdir(d)) {
		struct stat st;
		char *fname = (char *)malloc(64);
		if (strcmp(de->d_name, ".") == 0 ||
		    strcmp(de->d_name, "..") == 0) {
			continue;
		}
		snprintf(fname, 64, "%s/%s", op->fname, de->d_name);
		if (fname == NULL) {
			printf("Out of memory\n");
			exit(1);
		}
		if (stat(fname, &st) != 0) {
			continue;
		}
		if (S_ISDIR(st.st_mode)) {
			struct dbench_op op2 = *op;
			op2.fname = fname;
			fio_deltree(&op2);
		} else {
			if (unlink(fname) != 0) {
				printf("[%d] unlink '%s' failed\n",
				       op->child->line, fname);
			}
		}
		free(fname);
	}
	closedir(d);
#endif
}

static void fio_cleanup(struct child_struct *child)
{
	char *dname = (char *)malloc(64);
	struct dbench_op op;

	ZERO_STRUCT(op);

	snprintf(dname, 64, "%s/clients/client%d", child->directory, child->id);
	op.child = child;
	op.fname = dname;
	fio_deltree(&op);
	free(dname);

	dname = (char *)malloc(64);
	snprintf(dname, 64, "%s%s", child->directory, "/clients");
	rmdir(dname);
	free(dname);
}

static struct backend_op ops[] = {
	{ "Deltree", fio_deltree },
	{ "Flush", fio_flush },
	{ "Close", fio_close },
	{ "Rmdir", fio_rmdir },
	{ "Mkdir", fio_mkdir },
	{ "Rename", fio_rename },
	{ "ReadX", fio_readx },
	{ "WriteX", fio_writex },
	{ "Unlink", fio_unlink },
	{ "FIND_FIRST", fio_findfirst },
	{ "QUERY_FILE_INFORMATION", fio_qfileinfo },
	{ "QUERY_PATH_INFORMATION", fio_qpathinfo },
	{ "NTCreateX", fio_createx },
	{ NULL, NULL}
};

struct nb_operations fileio_ops = {
	.backend_name = "dbench",
	.setup 		= fio_setup,
	.cleanup	= fio_cleanup,
	.ops          = ops
};
