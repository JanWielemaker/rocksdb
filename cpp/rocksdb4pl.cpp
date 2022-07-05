/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2022, VU University Amsterdam
			 SWI-Prolog Solutions b.v.
    All rights reserved.

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions
    are met:

    1. Redistributions of source code must retain the above copyright
       notice, this list of conditions and the following disclaimer.

    2. Redistributions in binary form must reproduce the above copyright
       notice, this list of conditions and the following disclaimer in
       the documentation and/or other materials provided with the
       distribution.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
    FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
    COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
    POSSIBILITY OF SUCH DAMAGE.
*/

#include <assert.h>
#include <mutex>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/merge_operator.h>
#define PL_ARITY_AS_SIZE 1
#include <SWI-Stream.h>
#include <SWI-Prolog.h>
#include <SWI-cpp.h>

using namespace rocksdb;

		 /*******************************
		 *	       SYMBOL		*
		 *******************************/

#define DB_DESTROYED	0x0001		/* Was destroyed by user  */
#define DB_OPEN_ONCE	0x0002		/* open(once) option */

typedef enum
{ BLOB_ATOM = 0,			/* UTF-8 string as atom */
  BLOB_STRING,				/* UTF-8 string as string */
  BLOB_BINARY,				/* Byte string as string */
  BLOB_INT32,				/* 32-bit native integer */
  BLOB_INT64,				/* 64-bit native integer */
  BLOB_FLOAT32,				/* 32-bit IEEE float */
  BLOB_FLOAT64,				/* 64-bit IEEE double */
  BLOB_TERM				/* Arbitrary term */
} blob_type;

typedef enum
{ MERGE_NONE = 0,
  MERGE_LIST,
  MERGE_SET
} merger_t;

typedef struct dbref
{ rocksdb::DB	*db;			/* DB handle */
  atom_t         symbol;		/* associated symbol */
  atom_t	 name;			/* alias name */
  int	         flags;			/* flags */
  merger_t	 builtin_merger;	/* C++ Merger */
  record_t	 merger;		/* merge option */
  struct
  { blob_type key;
    blob_type value;
  } type;
} dbref;


		 /*******************************
		 *	      ALIAS		*
		 *******************************/

#define NULL_ATOM (atom_t)0

typedef struct alias_cell
{ atom_t	name;
  atom_t	symbol;
  struct alias_cell *next;
} alias_cell;

#define ALIAS_HASH_SIZE 64

std::mutex alias_lock;
static unsigned int alias_size = ALIAS_HASH_SIZE;
static alias_cell *alias_entries[ALIAS_HASH_SIZE];

static unsigned int
atom_hash(atom_t a)
{ return (unsigned int)(a>>7) % alias_size;
}

static atom_t
rocks_get_alias(atom_t name)
{ for(alias_cell *c = alias_entries[atom_hash(name)];
      c;
      c = c->next)
  { if ( c->name == name )
      return c->symbol;
  }

  return NULL_ATOM;
}

static void
rocks_alias(atom_t name, atom_t symbol)
{ auto key = atom_hash(name);

  alias_lock.lock();
  if ( !rocks_get_alias(name) )
  { alias_cell *c = static_cast<alias_cell *>(malloc(sizeof *c));

    c->name   = name;
    c->symbol = symbol;
    c->next   = alias_entries[key];
    alias_entries[key] = c;
    PL_register_atom(c->name);
    PL_register_atom(c->symbol);
    alias_lock.unlock();
  } else
  { alias_lock.unlock();
    throw PlPermissionError("alias", "rocksdb", PlTerm(name));
  }
}

static void
rocks_unalias(atom_t name)
{ auto key = atom_hash(name);
  alias_cell *c, *prev=NULL;

  alias_lock.lock();
  for(c = alias_entries[key]; c; prev=c, c = c->next)
  { if ( c->name == name )
    { if ( prev )
	prev->next = c->next;
      else
	alias_entries[key] = c->next;
      PL_unregister_atom(c->name);
      PL_unregister_atom(c->symbol);
      free(c);

      break;
    }
  }
  alias_lock.unlock();
}


		 /*******************************
		 *	 SYMBOL REFERENCES	*
		 *******************************/

static int
write_rocks_ref(IOSTREAM *s, atom_t eref, int flags)
{ auto refp = static_cast<dbref **>(PL_blob_data(eref, NULL, NULL));
  auto ref  = *refp;
  (void)flags;

  Sfprintf(s, "<rocksdb>(%p)", ref);
  return TRUE;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
GC an rocks from the atom garbage collector.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
release_rocks_ref(atom_t aref)
{ auto refp = static_cast<dbref **>(PL_blob_data(aref, NULL, NULL));
  auto ref  = *refp;

  assert(ref->name == NULL_ATOM);

  { auto db = ref->db;
    if ( db )
    { ref->db = NULL;
      delete db;
    }
  }
  if ( ref->merger )
  { PL_erase(ref->merger);
    ref->merger = 0;
  }
  PL_free(ref);

  return TRUE;
}


static int
save_rocks(atom_t aref, IOSTREAM *fd)
{ auto refp = static_cast<dbref **>(PL_blob_data(aref, NULL, NULL));
  auto ref  = *refp;
  (void)fd;

  return PL_warning("Cannot save reference to <rocksdb>(%p)", ref);
}


static atom_t
load_rocks(IOSTREAM *fd)
{ (void)fd;

  return PL_new_atom("<saved-rocksdb-ref>");
}


static PL_blob_t rocks_blob =
{ PL_BLOB_MAGIC,
  PL_BLOB_UNIQUE,
  (const char *)"rocksdb",
  release_rocks_ref,
  NULL,
  write_rocks_ref,
  NULL,
  save_rocks,
  load_rocks
};


static int
unify_rocks(term_t t, dbref *ref)
{ if ( ref->name )
  { if ( !ref->symbol )
    { PlTerm tmp;

      if ( PL_unify_blob(tmp, &ref, sizeof ref, &rocks_blob) &&
	   PL_get_atom(tmp, &ref->symbol) )
      { rocks_alias(ref->name, ref->symbol);
      } else
      { assert(0);
      }
    }
    return PL_unify_atom(t, ref->name);
  } else if ( ref->symbol )
  { return PL_unify_atom(t, ref->symbol);
  } else
  { return ( PL_unify_blob(t, &ref, sizeof ref, &rocks_blob) &&
	     PL_get_atom(t, &ref->symbol)
	   );
  }
}


static dbref*
symbol_dbref(atom_t symbol)
{ void *data;
  size_t len;
  PL_blob_t *type;

  if ( (data=PL_blob_data(symbol, &len, &type)) && type == &rocks_blob )
  { auto erd = static_cast<dbref **>(data);
    return *erd;
  }

  return static_cast<dbref *>(NULL);
}


static int
get_rocks(term_t t, dbref **erp, int warn=TRUE)
{ atom_t a;

  if ( PL_get_atom(t, &a) )
  { for(int i=0; i<2; i++)
    { dbref *ref;

      if ( (ref=symbol_dbref(a)) )
      { if ( !(ref->flags & DB_DESTROYED) )
	{ *erp = ref;
	  return TRUE;
	} else if ( warn )
	{ throw PlExistenceError("rocksdb", t);
	}
      }

      a=rocks_get_alias(a);
    }

    throw PlExistenceError("rocksdb", t);
  }

  if ( warn )
    throw PlTypeError("rocksdb", t);

  return FALSE;
}


		 /*******************************
		 *	      UTIL		*
		 *******************************/

class RocksError : public PlException
{
public:
  RocksError(const rocksdb::Status &status) :
    PlException(PlCompound("error",
			   PlTermv(PlCompound("rocks_error",
					      PlTerm(status.ToString().c_str())),
				   PlTerm())))
  {
  }
};


static int
ok(const rocksdb::Status &status)
{ if ( status.ok() )
    return TRUE;
  if ( status.IsNotFound() )
    return FALSE;
  throw RocksError(status);
}

class PlSlice : public Slice
{
public:
  int must_free = 0;
  union
  { int     i32;  // int32_t
    int64_t i64;
    float   f32;
    double  f64;
  } v;

  void clear()
  { if ( must_free )
      PL_erase_external(const_cast<char *>(data_));
    must_free = 0;
    data_ = NULL;
    size_ = 0;
  }

  ~PlSlice()
  { if ( must_free )
      PL_erase_external(const_cast<char *>(data_));
  }
};


#define CVT_IN	(CVT_ATOM|CVT_STRING|CVT_LIST)

static void
get_slice(term_t t, PlSlice &s, blob_type type)
{ char *str;
  size_t len;

  switch(type)
  { case BLOB_ATOM:
    case BLOB_STRING:
      if ( PL_get_nchars(t, &len, &str, CVT_IN|CVT_EXCEPTION|REP_UTF8) )
      { s.data_ = str;
	s.size_ = len;
	return;
      }
      throw(PlException(PL_exception(0)));
    case BLOB_BINARY:
      if ( PL_get_nchars(t, &len, &str, CVT_IN|CVT_EXCEPTION) )
      { s.data_ = str;
	s.size_ = len;
	return;
      }
      throw(PlException(PL_exception(0)));
    case BLOB_INT32:
    { if ( PL_get_integer_ex(t, &s.v.i32) )
      { s.data_ = reinterpret_cast<const char *>(&s.v.i32);
	s.size_ = sizeof s.v.i32;
	return;
      }
      throw(PlException(PL_exception(0)));
    }
    case BLOB_INT64:
    { if ( PL_get_int64_ex(t, &s.v.i64) )
      { s.data_ = reinterpret_cast<const char *>(&s.v.i64);
	s.size_ = sizeof s.v.i64;
	return;
      }
      throw(PlException(PL_exception(0)));
    }
    case BLOB_FLOAT32:
    { double d;

      if ( PL_get_float_ex(t, &d) )
      { s.v.f32 = d;
	s.data_ = reinterpret_cast<const char *>(&s.v.f32);
	s.size_ = sizeof s.v.f32 ;
	return;
      }
      throw(PlException(PL_exception(0)));
    }
    case BLOB_FLOAT64:
    { if ( PL_get_float_ex(t, &s.v.f64) )
      { s.data_ = reinterpret_cast<const char*>(&s.v.f64);
	s.size_ = sizeof s.v.f64;
	return;
      }
      throw(PlException(PL_exception(0)));
    }
    case BLOB_TERM:
      if ( (str=PL_record_external(t, &len)) )
      { s.data_ = str;
	s.size_ = len;
	s.must_free = TRUE;
	return;
      }
      throw(PlException(PL_exception(0)));
    default:
      assert(0);
  }
}


static const PlAtom ATOM_("");


static int
unify(term_t t, const Slice &s, blob_type type)
{ switch(type)
  { case BLOB_ATOM:
      return PL_unify_chars(t, PL_ATOM|REP_UTF8, s.size_, s.data_);
    case BLOB_STRING:
      return PL_unify_chars(t, PL_STRING|REP_UTF8, s.size_, s.data_);
    case BLOB_BINARY:
      return PL_unify_chars(t, PL_STRING|REP_ISO_LATIN_1, s.size_, s.data_);
    case BLOB_INT32:
    { int i;

      memcpy(&i, s.data_, sizeof i); // Unaligned i=*reinterpret_cast<int>(s.data_)
      return PL_unify_integer(t, i);
    }
    case BLOB_INT64:
    { int64_t i;

      memcpy(&i, s.data_, sizeof i);
      return PL_unify_int64(t, i);
    }
    case BLOB_FLOAT32:
    { float f;

      memcpy(&f, s.data_, sizeof f);
      return PL_unify_float(t, f);
    }
    case BLOB_FLOAT64:
    { double f;

      memcpy(&f, s.data_, sizeof f);
      return PL_unify_float(t, f);
    }
    case BLOB_TERM:
    { PlTerm tmp;

      return ( PL_recorded_external(s.data_, tmp) &&
	       PL_unify(tmp, t)
	     );
    }
    default:
      assert(0);
      return FALSE;
  }
}

static int
unify(term_t t, const Slice *s, blob_type type)
{ if ( s == static_cast<const Slice *>(NULL) )
  { switch(type)
    { case BLOB_ATOM:
	return PL_unify_atom(t, ATOM_.handle);
      case BLOB_STRING:
      case BLOB_BINARY:
	return PL_unify_chars(t, PL_STRING, 0, "");
      case BLOB_INT32:
      case BLOB_INT64:
	return PL_unify_integer(t, 0);
      case BLOB_FLOAT32:
      case BLOB_FLOAT64:
	return PL_unify_float(t, 0.0);
      case BLOB_TERM:
	return PL_unify_nil(t);
      default:
	assert(0);
	return FALSE;
    }
  }

  return unify(t, *s, type);
}

static int
unify(term_t t, const std::string &s, blob_type type)
{ Slice sl(s.data(), s.length());

  return unify(t, sl, type);
}

static int
unify_value(term_t t, const Slice &s, merger_t merge, blob_type type)
{ if ( merge == MERGE_NONE )
  { return unify(t, s, type);
  } else
  { PlTail list(t);
    PlTerm tmp;
    const char *data = s.data();
    const char *end  = data+s.size();
    int rc;

    while(data < end)
    { switch( type )
      { case BLOB_INT32:
	{ int i;
	  memcpy(&i, data, sizeof i);
	  data += sizeof i;
	  rc = PL_put_integer(tmp, i);
	  break;
	}
	case BLOB_INT64:
	{ int64_t i;
	  memcpy(&i, data, sizeof i);
	  data += sizeof i;
	  rc = PL_put_int64(tmp, i);
	  break;
	}
	case BLOB_FLOAT32:
	{ float i;
	  memcpy(&i, data, sizeof i);
	  data += sizeof i;
	  rc = PL_put_float(tmp, i);
	  break;
	}
	case BLOB_FLOAT64:
	{ double i;
	  memcpy(&i, data, sizeof i);
	  data += sizeof i;
	  rc = PL_put_float(tmp, i);
	  break;
	}
	default:
	  assert(0);
	  rc = FALSE;
      }

      if ( !list.append(tmp) )
	return FALSE;
    }

    return list.close();
  }
}

static int
unify_value(term_t t, const std::string &s, merger_t merge, blob_type type)
{ Slice sl(s.data(), s.length());

  return unify_value(t, sl, merge, type);
}


		 /*******************************
		 *	       MERGER		*
		 *******************************/

static const PlAtom ATOM_partial("partial");
static const PlAtom ATOM_full("full");

static int
log_exception(Logger* logger)
{ PlException ex(PL_exception(0));

  Log(logger, "%s", static_cast<const char *>(ex));
  return false;
}

class engine
{ int tid = 0;

public:
  engine()
  { if ( PL_thread_self() == -1 )
    { if ( (tid=PL_thread_attach_engine(NULL)) < 0 )
      { term_t ex;

	if ( (ex = PL_exception(0)) )
	  PlException(ex).cppThrow();
	else
	  throw PlResourceError("memory");
      }
    }
  }

  ~engine()
  { if ( tid )
      PL_thread_destroy_engine();
  }
};

static int
call_merger(const dbref *ref, PlTermv av, std::string* new_value,
	    Logger* logger)
{ static predicate_t pred_call6 = NULL;

  if ( !pred_call6 )
    pred_call6 = PL_predicate("call", 6, "system");

  try
  { PlQuery q(pred_call6, av);
    if ( q.next_solution() )
    { PlSlice answer;

      get_slice(av[5], answer, ref->type.value);
      new_value->assign(answer.data(), answer.size());
      return true;
    } else
    { Log(logger, "merger failed");
      return false;
    }
  } catch(PlException &ex)
  { Log(logger, "%s", static_cast<const char *>(ex));
    return false;
  }
}


class PrologMergeOperator : public MergeOperator
{ const dbref *ref;
public:
  PrologMergeOperator(const dbref *reference) : MergeOperator()
  { ref = reference;
  }

  virtual bool
  FullMerge(const Slice& key,
	    const Slice* existing_value,
	    const std::deque<std::string>& operand_list,
	    std::string* new_value,
	    Logger* logger) const override
  { engine e;
    PlTermv av(6);
    PlTail list(av[4]);
    PlTerm tmp;

    for (const auto& value : operand_list)
    { PL_put_variable(tmp);
      unify(tmp, value, ref->type.value);
      list.append(tmp);
    }
    list.close();

    if ( PL_recorded(ref->merger, av[0]) &&
	 (av[1] = ATOM_full) &&
	 unify(av[2], key, ref->type.key) &&
	 unify(av[3], existing_value, ref->type.value) )
      return call_merger(ref, av, new_value, logger);
    else
      return log_exception(logger);
  }

  virtual bool
  PartialMerge(const Slice& key,
	       const Slice& left_operand,
	       const Slice& right_operand,
	       std::string* new_value,
	       Logger* logger) const override
  { engine e;
    PlTermv av(6);

    if ( PL_recorded(ref->merger, av[0]) &&
	 (av[1] = ATOM_partial) &&
	 unify(av[2], key, ref->type.key) &&
	 unify(av[3], left_operand, ref->type.value) &&
	 unify(av[4], right_operand, ref->type.value) )
      return call_merger(ref, av, new_value, logger);
    else
      return log_exception(logger);
  }

  virtual const char*
  Name() const override
  { return "PrologMergeOperator";
  }
};

static int
cmp_int32(const void *v1, const void *v2)
{ auto i1 = static_cast<const int *>(v1);
  auto i2 = static_cast<const int *>(v2);

  return *i1 > *i2 ? 1 : *i1 < *i2 ? -1 : 0;
}


void
sort(std::string &str, blob_type type)
{ auto s = const_cast<char *>(str.c_str());
  auto len = str.length();

  if ( len > 0 )
  { switch(type)
    { case BLOB_INT32:
      { auto ip = reinterpret_cast<int *>(s);
	auto op = ip+1;
	auto ep = reinterpret_cast<int *>(s+len);
	int cv;

	qsort(s, len/sizeof(int), sizeof(int), cmp_int32);
	cv = *ip;
	for(ip++; ip < ep; ip++)
	{ if ( *ip != cv )
	    *op++ = cv = *ip;
	}
	str.resize(reinterpret_cast<char *>(op)-s);
	break;
      }
      case BLOB_INT64:
      { auto ip = reinterpret_cast<int64_t *>(s);
	auto op = ip+1;
	auto ep = reinterpret_cast<int64_t *>(s+len);
	int64_t cv;

	qsort(s, len/sizeof(int64_t), sizeof(int64_t), cmp_int32);
	cv = *ip;
	for(ip++; ip < ep; ip++)
	{ if ( *ip != cv )
	    *op++ = cv = *ip;
	}
	str.resize(reinterpret_cast<char *>(op)-s);
	break;
      }
      case BLOB_FLOAT32:
      { auto ip = reinterpret_cast<float *>(s);
	auto op = ip+1;
	auto ep = reinterpret_cast<float *>(s+len);
	float cv;

	qsort(s, len/sizeof(float), sizeof(float), cmp_int32);
	cv = *ip;
	for(ip++; ip < ep; ip++)
	{ if ( *ip != cv )
	    *op++ = cv = *ip;
	}
	str.resize(reinterpret_cast<char *>(op)-s);
	break;
      }
      case BLOB_FLOAT64:
      { auto ip = reinterpret_cast<double *>(s);
	auto op = ip+1;
	auto ep = reinterpret_cast<double *>(s+len);
	double cv;

	qsort(s, len/sizeof(double), sizeof(double), cmp_int32);
	cv = *ip;
	for(ip++; ip < ep; ip++)
	{ if ( *ip != cv )
	    *op++ = cv = *ip;
	}
	str.resize(reinterpret_cast<char *>(op)-s);
	break;
      }
      default:
	assert(0);
	return;
    }
  }
}


class ListMergeOperator : public MergeOperator
{ const dbref *ref;
public:
  ListMergeOperator(const dbref *reference) : MergeOperator()
  { ref = reference;
  }

  virtual bool
  FullMerge(const Slice& key,
	    const Slice* existing_value,
	    const std::deque<std::string>& operand_list,
	    std::string* new_value,
	    Logger* logger) const override
  { std::string s;

    if ( existing_value )
      s += existing_value->ToString();

    for (const auto& value : operand_list)
    { s += value;
    }

    if ( ref->builtin_merger == MERGE_SET )
      sort(s, ref->type.value);
    *new_value = s;
    return true;
  }

  virtual bool
  PartialMerge(const Slice& key,
	       const Slice& left_operand,
	       const Slice& right_operand,
	       std::string* new_value,
	       Logger* logger) const override
  { std::string s = left_operand.ToString();
    s += right_operand.ToString();

    if ( ref->builtin_merger == MERGE_SET )
      sort(s, ref->type.value);
    *new_value = s;
    return true;
  }

  virtual const char*
  Name() const override
  { return "ListMergeOperator";
  }
};


		 /*******************************
		 *	    PREDICATES		*
		 *******************************/

static PlAtom ATOM_key("key");
static PlAtom ATOM_value("value");
static PlAtom ATOM_alias("alias");
static PlAtom ATOM_merge("merge");

static PlAtom ATOM_atom("atom");
static PlAtom ATOM_string("string");
static PlAtom ATOM_binary("binary");
static PlAtom ATOM_int32("int32");
static PlAtom ATOM_int64("int64");
static PlAtom ATOM_float("float");
static PlAtom ATOM_double("double");
static PlAtom ATOM_term("term");
static PlAtom ATOM_open("open");
static PlAtom ATOM_once("once");
static PlAtom ATOM_mode("mode");
static PlAtom ATOM_read_only("read_only");
static PlAtom ATOM_read_write("read_write");

static PlFunctor FUNCTOR_list1("list", 1);
static PlFunctor FUNCTOR_set1("set", 1);

static void
get_blob_type(PlTerm t, blob_type *key_type, merger_t *m)
{ atom_t a;
  int rc;

  if ( m && PL_is_functor(t, FUNCTOR_list1) )
  { *m = MERGE_LIST;
    rc = PL_get_atom(t[1], &a);
  } else if ( m && PL_is_functor(t, FUNCTOR_set1) )
  { *m = MERGE_SET;
    rc = PL_get_atom(t[1], &a);
  } else
    rc = PL_get_atom(t, &a);

  if ( rc )
  { if ( !m || *m == MERGE_NONE )
    {      if ( ATOM_atom   == a ) { *key_type = BLOB_ATOM;   return; }
      else if ( ATOM_string == a ) { *key_type = BLOB_STRING; return; }
      else if ( ATOM_binary == a ) { *key_type = BLOB_BINARY; return; }
      else if ( ATOM_term   == a ) { *key_type = BLOB_TERM;   return; }
    }

         if ( ATOM_int32  == a ) { *key_type = BLOB_INT32;   return; }
    else if ( ATOM_int64  == a ) { *key_type = BLOB_INT64;   return; }
    else if ( ATOM_float  == a ) { *key_type = BLOB_FLOAT32; return; }
    else if ( ATOM_double == a ) { *key_type = BLOB_FLOAT64; return; }
    else throw PlDomainError("rocks_type", t);

    return;
  }

  throw PlTypeError("atom", t);
}


typedef void (*ReadOptdefAction)(rocksdb::ReadOptions *options, PlTerm t);

struct ReadOptdef
{ const char *name;
  ReadOptdefAction action;
  atom_t atom; // Initially 0; filled in as-needed by lookup
};

#define RD_ODEF [](rocksdb::ReadOptions *options, PlTerm arg)

static ReadOptdef read_optdefs[] =
{ // "snapshot" const Snapshot*
  // "iterate_lower_bound" const Slice*
  // "iterate_upper_bound" const Slice*
  {         "readahead_size",                       RD_ODEF {
    options->readahead_size                       = static_cast<size_t>(arg); } },
  {         "max_skippable_internal_keys",          RD_ODEF {
    options->max_skippable_internal_keys          = static_cast<uint64_t>(arg); } },
  // "read_tier" ReadTier
  {         "verify_checksums",                     RD_ODEF {
    options->verify_checksums                     = static_cast<bool>(arg); } },
  {         "fill_cache",                           RD_ODEF {
    options->fill_cache                           = static_cast<bool>(arg); } },
  {         "tailing",                              RD_ODEF {
    options->tailing                              = static_cast<bool>(arg); } },
  // "managed" - not used any more
  {         "total_order_seek",                     RD_ODEF {
    options->total_order_seek                     = static_cast<bool>(arg); } },
  {         "auto_prefix_mode",                     RD_ODEF {
    options->auto_prefix_mode                     = static_cast<bool>(arg); } },
  {         "prefix_same_as_start",                 RD_ODEF {
    options->prefix_same_as_start                 = static_cast<bool>(arg); } },
  {         "pin_data",                             RD_ODEF {
    options->pin_data                             = static_cast<bool>(arg); } },
  {         "background_purge_on_iterator_cleanup", RD_ODEF {
    options->background_purge_on_iterator_cleanup = static_cast<bool>(arg); } },
  {         "ignore_range_deletions",               RD_ODEF {
    options->ignore_range_deletions               = static_cast<bool>(arg); } },
  // "table_filter" std::function<bool(const TableProperties&)>
  // TODO: "iter_start_seqnum" removed from rocksdb/options.h?
  // {         "iter_start_seqnum",                    RD_ODEF {
  //   options->iter_start_seqnum                    = static_cast<SequenceNumber>(arg); } },
  //         "timestamp" Slice*
  // "iter_start_ts" Slice*
  {
             "deadline",                            RD_ODEF {
    options->deadline                             = static_cast<std::chrono::microseconds>(arg); } },
  {         "io_timeout",                           RD_ODEF {
    options->io_timeout                           = static_cast<std::chrono::microseconds>(arg); } },
  {         "value_size_soft_limit",                RD_ODEF {
    options->value_size_soft_limit                = static_cast<uint64_t>(arg); } },

  { NULL }
};


// TODO: consolidate lookup_read_optdef_and_apply,
//       lookup_write_optdef_and_apply, lookup_optdef_and_apply;
//       possibly using STL map (although that doesn't support
//       lazy filling in of the atoms in the lookup table)
static void
lookup_read_optdef_and_apply(rocksdb::ReadOptions *options,
			     ReadOptdef opt_defs[],
			     atom_t name, PlTerm opt)
{ for(auto def=opt_defs; def->name; def++)
  { if ( !def->atom ) // lazilly fill in atoms in lookup table
      def->atom = PL_new_atom(def->name);
    if ( def->atom == name )
    { def->action(options, opt[1]);
      return;
    }
  }
  throw PlTypeError("option", opt);
}


typedef void (*WriteOptdefAction)(rocksdb::WriteOptions *options, PlTerm t);

struct WriteOptdef
{ const char *name;
  WriteOptdefAction action;
  atom_t atom; // Initially 0; filled in as-needed by lookup
};

#define WR_ODEF [](rocksdb::WriteOptions *options, PlTerm arg)

static WriteOptdef write_optdefs[] =
{ {         "sync",                           WR_ODEF {
    options->sync                           = static_cast<bool>(arg); } },
  {         "disableWAL",                     WR_ODEF {
    options->disableWAL                     = static_cast<bool>(arg); } },
  {         "ignore_missing_column_families", WR_ODEF {
    options->ignore_missing_column_families = static_cast<bool>(arg); } },
  {         "no_slowdown",                    WR_ODEF {
    options->no_slowdown                    = static_cast<bool>(arg); } },
  {         "low_pri",                        WR_ODEF {
    options->low_pri                        = static_cast<bool>(arg); } },
  {         "memtable_insert_hint_per_batch", WR_ODEF {
    options->memtable_insert_hint_per_batch = static_cast<bool>(arg); } },
  // "timestamp" Slice*

{ NULL }
};


static void
lookup_write_optdef_and_apply(rocksdb::WriteOptions *options,
			      WriteOptdef opt_defs[],
			      atom_t name, PlTerm opt)
{ for(auto def=opt_defs; def->name; def++)
  { if ( !def->atom ) // lazilly fill in atoms in lookup table
      def->atom = PL_new_atom(def->name);
    if ( def->atom == name )
    { def->action(options, opt[1]);
      return;
    }
  }
  throw PlTypeError("option", opt);
}

static void
lookup_InfoLogLevel(rocksdb::Options *options, PlTerm arg)
{ const auto str = static_cast<std::string>(arg);
  if ( str == "debug"  ) { options->info_log_level = DEBUG_LEVEL;  return; }
  if ( str == "info"   ) { options->info_log_level = INFO_LEVEL;   return; }
  if ( str == "warn"   ) { options->info_log_level = WARN_LEVEL;   return; }
  if ( str == "error"  ) { options->info_log_level = ERROR_LEVEL;  return; }
  if ( str == "fatal"  ) { options->info_log_level = FATAL_LEVEL;  return; }
  if ( str == "header" ) { options->info_log_level = HEADER_LEVEL; return; }
  throw PlTypeError("InfoLogLevel", arg);
}


typedef void (*OptdefAction)(rocksdb::Options *options, PlTerm t);

struct Optdef
{ const char *name;
  OptdefAction action;
  atom_t atom; // Initially 0; filled in as-needed by lookup
};

#define ODEF [](rocksdb::Options *options, PlTerm arg)

static Optdef optdefs[] =
{ { "prepare_for_bulk_load",                           ODEF { if ( static_cast<bool>(arg) ) options->PrepareForBulkLoad(); } }, // TODO: what to do with false?
  { "optimize_for_small_db",                           ODEF { if ( static_cast<bool>(arg) ) options->OptimizeForSmallDb(); } }, // TODO: what to do with false? - there's no DontOptimizeForSmallDb()
#ifndef ROCKSDB_LITE
  { "increase_parallelism",                            ODEF { if ( static_cast<bool>(arg) ) options->IncreaseParallelism(); } },
#endif
  {         "create_if_missing",                       ODEF {
    options->create_if_missing                       = static_cast<bool>(arg); } },
  {         "create_missing_column_families",          ODEF {
    options->create_missing_column_families          = static_cast<bool>(arg); } },
  {         "error_if_exists",                         ODEF {
    options->error_if_exists                         = static_cast<bool>(arg); } },
  {         "paranoid_checks",                         ODEF {
    options->paranoid_checks                         = static_cast<bool>(arg); } },
  {         "track_and_verify_wals_in_manifest",       ODEF {
    options->track_and_verify_wals_in_manifest       = static_cast<bool>(arg); } },
  // "env" Env::Default
  // "rate_limiter" - shared_ptr<RateLimiter>
  // "sst_file_manager" - shared_ptr<SstFileManager>

  // "info_log" - shared_ptr<Logger> // TODO: allow specifying a callback and/or stream
  { "info_log_level",                                  lookup_InfoLogLevel },
  {         "max_open_files",                          ODEF {
    options->max_open_files                          = static_cast<int>(arg); } },
  {         "max_file_opening_threads",                ODEF {
    options-> max_file_opening_threads               = static_cast<int>(arg); } },
  {         "max_total_wal_size",                      ODEF {
    options->max_total_wal_size                      = static_cast<uint64_t>(arg); } },
  // "statistics" - shared_ptr<Statistics>
  {         "use_fsync",                               ODEF {
    options->use_fsync                               = static_cast<bool>(arg); } },
  // "db_paths" - vector<DbPath>
  {         "db_log_dir",                              ODEF {
    options->db_log_dir                              = static_cast<std::string>(arg); } },
  {         "wal_dir",                                 ODEF {
    options->wal_dir                                 = static_cast<std::string>(arg); } },
  {         "delete_obsolete_files_period_micros",     ODEF {
    options->delete_obsolete_files_period_micros     = static_cast<uint64_t>(arg); } },
  {         "max_background_jobs",                     ODEF {
    options->max_background_jobs                     = static_cast<uint64_t>(arg); } },
  // base_background_compactions is obsolete
  // max_background_compactions is obsolete
  {         "max_subcompactions",                      ODEF {
    options->max_subcompactions                      = static_cast<uint32_t>(arg); } },
  // max_background_flushes is obsolete
  {         "max_log_file_size",                       ODEF {
    options->max_log_file_size                       = static_cast<size_t>(arg); } },
  {         "log_file_time_to_roll",                   ODEF {
    options->log_file_time_to_roll                   = static_cast<size_t>(arg); } },
  {         "keep_log_file_num",                       ODEF {
    options->keep_log_file_num                       = static_cast<size_t>(arg); } },
  {         "recycle_log_file_num",                    ODEF {
    options->recycle_log_file_num                    = static_cast<size_t>(arg); } },
  {         "max_manifest_file_size",                  ODEF {
    options->max_manifest_file_size                  = static_cast<uint64_t>(arg); } },
  {         "table_cache_numshardbits",                ODEF {
    options->table_cache_numshardbits                = static_cast<int>(arg); } },
  {         "wal_ttl_seconds",                         ODEF {
    options->WAL_ttl_seconds                         = static_cast<uint64_t>(arg); } },
  {         "wal_size_limit_mb",                       ODEF {
    options->WAL_size_limit_MB                       = static_cast<uint64_t>(arg); } },
  {         "manifest_preallocation_size",             ODEF {
    options->manifest_preallocation_size             = static_cast<size_t>(arg); } },
  {         "allow_mmap_reads",                        ODEF {
    options->allow_mmap_reads                        = static_cast<bool>(arg); } },
  {         "allow_mmap_writes",                       ODEF {
    options->allow_mmap_writes                       = static_cast<bool>(arg); } },
  {         "use_direct_reads",                        ODEF {
    options->use_direct_reads                        = static_cast<bool>(arg); } },
  {         "use_direct_io_for_flush_and_compaction",  ODEF {
    options->use_direct_io_for_flush_and_compaction  = static_cast<bool>(arg); } },
  {         "allow_fallocate",                         ODEF {
    options->allow_fallocate                         = static_cast<bool>(arg); } },
  {         "is_fd_close_on_exec",                     ODEF {
    options->is_fd_close_on_exec                     = static_cast<bool>(arg); } },
  // skip_log_error_on_recovery is obsolete
  {         "stats_dump_period_sec",                   ODEF {
    options->stats_dump_period_sec                   = static_cast<unsigned int>(arg); } },
  {         "stats_persist_period_sec",                ODEF {
    options->stats_persist_period_sec                = static_cast<unsigned int>(arg); } },
  {         "persist_stats_to_disk",                   ODEF {
    options->persist_stats_to_disk                   = static_cast<bool>(arg); } },
  {         "stats_history_buffer_size",               ODEF {
    options->stats_history_buffer_size               = static_cast<size_t>(arg); } },
  {         "advise_random_on_open",                   ODEF {
    options->advise_random_on_open                   = static_cast<bool>(arg); } },
  {         "db_write_buffer_size",                    ODEF {
    options->db_write_buffer_size                    = static_cast<size_t>(arg); } },
  // "write_buffer_manager" - shared_ptr<WriteBufferManager>
  // "access_hint_on_compaction_start" - enum AccessHint
  // TODO: "new_table_reader_for_compaction_inputs"  removed from rocksdb/options.h?
  // {         "new_table_reader_for_compaction_inputs",  ODEF {
  //   options->new_table_reader_for_compaction_inputs  = static_cast<bool>(arg); } },
  {         "compaction_readahead_size",               ODEF {
    options->compaction_readahead_size               = static_cast<size_t>(arg); } },
  {         "random_access_max_buffer_size",           ODEF {
    options->random_access_max_buffer_size           = static_cast<size_t>(arg); } },
  {         "writable_file_max_buffer_size",           ODEF {
    options->writable_file_max_buffer_size           = static_cast<size_t>(arg); } },
  {         "use_adaptive_mutex",                      ODEF {
    options->use_adaptive_mutex                      = static_cast<bool>(arg); } },
  {         "bytes_per_sync",                          ODEF {
    options->bytes_per_sync                          = static_cast<uint64_t>(arg); } },
  {         "wal_bytes_per_sync",                      ODEF {
    options->wal_bytes_per_sync                      = static_cast<uint64_t>(arg); } },
  {         "strict_bytes_per_sync",                   ODEF {
    options->strict_bytes_per_sync                   = static_cast<bool>(arg); } },
  // "listeners" - vector<shared_ptr<EventListener>>
  {         "enable_thread_tracking",                  ODEF {
    options->enable_thread_tracking                  = static_cast<bool>(arg); } },
  {         "delayed_write_rate",                      ODEF {
    options->delayed_write_rate                      = static_cast<uint64_t>(arg); } },
  {         "enable_pipelined_write",                  ODEF {
    options->enable_pipelined_write                  = static_cast<bool>(arg); } },
  {         "unordered_write",                         ODEF {
    options->unordered_write                         = static_cast<bool>(arg); } },
  {         "allow_concurrent_memtable_write",         ODEF {
    options->allow_concurrent_memtable_write         = static_cast<bool>(arg); } },
  {         "enable_write_thread_adaptive_yield",      ODEF {
    options->enable_write_thread_adaptive_yield      = static_cast<bool>(arg); } },
  {         "max_write_batch_group_size_bytes",        ODEF {
    options->max_write_batch_group_size_bytes        = static_cast<uint64_t>(arg); } },
  {         "write_thread_max_yield_usec",             ODEF {
    options->write_thread_max_yield_usec             = static_cast<uint64_t>(arg); } },
  {         "write_thread_slow_yield_usec",            ODEF {
    options->write_thread_slow_yield_usec            = static_cast<uint64_t>(arg); } },
  {         "skip_stats_update_on_db_open",            ODEF {
    options->skip_stats_update_on_db_open            = static_cast<bool>(arg); } },
  {         "skip_checking_sst_file_sizes_on_db_open", ODEF {
    options->skip_checking_sst_file_sizes_on_db_open = static_cast<bool>(arg); } },
  // "wal_recovery_mode" - enum WALRecoveryMode
  {         "allow_2pc",                               ODEF {
    options->allow_2pc                               = static_cast<bool>(arg); } },
  // "row_cache" - shared_ptr<Cache>
  // "wal_filter" - WalFilter*
  {         "fail_ifoptions_file_error",               ODEF {
    options->fail_if_options_file_error              = static_cast<bool>(arg); } },
  {         "dump_malloc_stats",                       ODEF {
    options->dump_malloc_stats                       = static_cast<bool>(arg); } },
  {         "avoid_flush_during_recovery",             ODEF {
    options->avoid_flush_during_recovery             = static_cast<bool>(arg); } },
  {         "avoid_flush_during_shutdown",             ODEF {
    options->avoid_flush_during_shutdown             = static_cast<bool>(arg); } },
  {         "allow_ingest_behind",                     ODEF {
    options->allow_ingest_behind                     = static_cast<bool>(arg); } },
  // TODO: "preserve_deletes" removed from rocksdb/options.h?
  // {         "preserve_deletes",                        ODEF {
  //   options->preserve_deletes                        = static_cast<bool>(arg); } },
  {         "two_write_queues",                        ODEF {
    options->two_write_queues                        = static_cast<bool>(arg); } },
  {         "manual_wal_flush",                        ODEF {
    options->manual_wal_flush                        = static_cast<bool>(arg); } },
  {         "atomic_flush",                            ODEF {
    options->atomic_flush                            = static_cast<bool>(arg); } },
  {         "avoid_unnecessary_blocking_io",           ODEF {
    options->avoid_unnecessary_blocking_io           = static_cast<bool>(arg); } },
  {         "write_dbid_to_manifest",                  ODEF {
    options->write_dbid_to_manifest                  = static_cast<bool>(arg); } },
  {         "log_readahead_size",                      ODEF {
    options->write_dbid_to_manifest                  = static_cast<bool>(arg); } },
  //         file_checksum_gen_factory
  { "best_efforts_recovery",                           ODEF {
    options->best_efforts_recovery                   = static_cast<bool>(arg); } },
  {         "max_bgerror_resume_count",                ODEF {
    options->max_bgerror_resume_count                = static_cast<int>(arg); } },
  {         "bgerror_resume_retry_interval",           ODEF {
    options->bgerror_resume_retry_interval           = static_cast<uint64_t>(arg); } },
  {         "allow_data_in_errors",                    ODEF {
    options->allow_data_in_errors                    = static_cast<bool>(arg); } },
  {         "db_host_id",                              ODEF {
    options->db_host_id                              = static_cast<std::string>(arg); } },
  // "checksum_handoff_file_types" - FileTypeSet

  { NULL }
};


static void
lookup_optdef_and_apply(rocksdb::Options *options,
			Optdef opt_defs[],
			atom_t name, PlTerm opt)
{ for(auto def=opt_defs; def->name; def++)
  { if ( !def->atom ) // lazilly fill in atoms in lookup table
      def->atom = PL_new_atom(def->name);
    if ( def->atom == name )
    { def->action(options, opt[1]);
      return;
    }
  }
  throw PlTypeError("option", opt);
}


PREDICATE(rocks_open_, 3)
{ rocksdb::Options options;
  options.create_if_missing = true;
  char *fn;
  blob_type key_type   = BLOB_ATOM;
  blob_type value_type = BLOB_ATOM;
  merger_t builtin_merger = MERGE_NONE;
  atom_t alias = NULL_ATOM;
  record_t merger = 0;
  int once = FALSE;
  int read_only = FALSE;

  if ( !PL_get_file_name(A1, &fn, PL_FILE_OSPATH) )
    return FALSE;
  PlTail tail(A3);
  PlTerm opt;
  while(tail.next(opt))
  { atom_t name;
    size_t arity;

    if ( PL_get_name_arity(opt, &name, &arity) && arity == 1 )
    { if ( ATOM_key == name )
	get_blob_type(opt[1], &key_type, static_cast<merger_t *>(NULL));
      else if ( ATOM_value == name )
	get_blob_type(opt[1], &value_type, &builtin_merger);
      else if ( ATOM_merge == name )
	merger = PL_record(opt[1]);
      else if ( ATOM_alias == name )
      { if ( !PL_get_atom_ex(opt[1], &alias) )
	  return FALSE;
	once = TRUE;
      } else if ( ATOM_open == name )
      { atom_t open;

	if ( !PL_get_atom_ex(opt[1], &open) )
	  return FALSE;
	if ( ATOM_once == open )
	  once = TRUE;
	else
	  throw PlDomainError("open_option", opt[1]);
      } else if ( ATOM_mode == name )
      { atom_t a;

	if ( PL_get_atom(opt[1], &a) )
	{ if ( ATOM_read_write == a )
	    read_only = FALSE;
	  else if ( ATOM_read_only == a )
	    read_only = TRUE;
	  else
	    throw PlDomainError("mode_option", opt[1]);
	} else
	  throw PlTypeError("atom", opt[1]);
      } else
      { lookup_optdef_and_apply(&options, optdefs, name, opt);
      }
    } else
      throw PlTypeError("option", opt);
  }

  if ( alias && once )
  { atom_t existing;

    if ( (existing=rocks_get_alias(alias)) )
    { dbref *eref;

      if ( (eref=symbol_dbref(existing)) &&
	   (eref->flags&DB_OPEN_ONCE) )
	return PL_unify_atom(A2, existing);
    }
  }

  dbref *ref = static_cast<dbref *>(PL_malloc(sizeof *ref));
  memset(ref, 0, sizeof *ref);
  ref->merger         = merger;
  ref->builtin_merger = builtin_merger;
  ref->type.key       = key_type;
  ref->type.value     = value_type;
  ref->name           = alias;
  if ( once )
    ref->flags |= DB_OPEN_ONCE;

  try
  { rocksdb::Status status;

    if ( ref->merger )
      options.merge_operator.reset(new PrologMergeOperator(ref));
    else if ( builtin_merger != MERGE_NONE )
      options.merge_operator.reset(new ListMergeOperator(ref));
    if ( read_only )
      status = DB::OpenForReadOnly(options, fn, &ref->db);
    else
      status = DB::Open(options, fn, &ref->db);
    ok(status);
    return unify_rocks(A2, ref);
  } catch(...)
  { PL_free(ref);
    throw;
  }
}


PREDICATE(rocks_close, 1)
{ dbref *ref;

  get_rocks(A1, &ref);
  DB* db = ref->db;

  ref->db = NULL;
  ref->flags |= DB_DESTROYED;
  if ( ref->name )
  { rocks_unalias(ref->name);
    ref->name = NULL_ATOM;
  }

  delete db;
  return TRUE;
}


static WriteOptions
write_options(PlTerm options_term)
{ WriteOptions options;
  PlTail tail(options_term);
  PlTerm opt;
  while(tail.next(opt))
  { atom_t name;
    size_t arity;

    if ( PL_get_name_arity(opt, &name, &arity) && arity == 1 )
     lookup_write_optdef_and_apply(&options, write_optdefs, name, opt);
    else
      throw PlTypeError("option", opt);
  }
  return options;
}


PREDICATE(rocks_put, 4)
{ dbref *ref;
  PlSlice key;

  get_rocks(A1, &ref);
  get_slice(A2, key,   ref->type.key);

  if ( ref->builtin_merger == MERGE_NONE )
  { PlSlice value;

    get_slice(A3, value, ref->type.value);
    ok(ref->db->Put(write_options(A4), key, value));
  } else
  { PlTail list(A3);
    PlTerm tmp;
    std::string value;
    PlSlice s;

    while(list.next(tmp))
    { get_slice(tmp, s, ref->type.value);
      value += s.ToString();
      s.clear();
    }

    if ( ref->builtin_merger == MERGE_SET )
      sort(value, ref->type.value);

    ok(ref->db->Put(write_options(A4), key, value));
  }

  return TRUE;
}

PREDICATE(rocks_merge, 4)
{ dbref *ref;
  PlSlice key, value;

  get_rocks(A1, &ref);
  if ( !ref->merger && ref->builtin_merger == MERGE_NONE )
    throw PlPermissionError("merge", "rocksdb", A1);

  get_slice(A2, key,   ref->type.key);
  get_slice(A3, value, ref->type.value);

  ok(ref->db->Merge(write_options(A4), key, value));

  return TRUE;
}

static ReadOptions
read_options(PlTerm options_term)
{ ReadOptions options;
  PlTail tail(options_term);
  PlTerm opt;
  while(tail.next(opt))
  { atom_t name;
    size_t arity;

    if ( PL_get_name_arity(opt, &name, &arity) && arity == 1 )
     lookup_read_optdef_and_apply(&options, read_optdefs, name, opt);
    else
      throw PlTypeError("option", opt);
  }
  return options;
}

PREDICATE(rocks_get, 4)
{ dbref *ref;
  PlSlice key;
  std::string value;

  get_rocks(A1, &ref);
  get_slice(A2, key, ref->type.key);

  return ( ok(ref->db->Get(read_options(A4), key, &value)) &&
	   unify_value(A3, value, ref->builtin_merger, ref->type.value) );
}

PREDICATE(rocks_delete, 3)
{ dbref *ref;
  PlSlice key;

  get_rocks(A1, &ref);
  get_slice(A2, key, ref->type.key);

  return ok(ref->db->Delete(write_options(A3), key));
}

typedef enum
{ ENUM_ALL,
  ENUM_FROM,
  ENUM_PREFIX
} enum_type;

typedef struct
{ Iterator *it;
  dbref    *ref;
  enum_type type;
  struct
  { size_t length;
    char  *string;
  } prefix;
  int saved;
} enum_state;

static enum_state *
save_enum_state(enum_state *state)
{ if ( !state->saved )
  { auto copy = static_cast<enum_state *>(malloc(sizeof (enum_state)));
    *copy = *state;
    if ( copy->prefix.string )
    { copy->prefix.string = static_cast<char *>(malloc(copy->prefix.length+1));
      memcpy(copy->prefix.string, state->prefix.string, copy->prefix.length+1);
    }
    copy->saved = TRUE;
    return copy;
  }

  return state;
}

static void
free_enum_state(enum_state *state)
{ if ( state->saved )
  { if ( state->it )
      delete state->it;
    if ( state->prefix.string )
      free(state->prefix.string);
    free(state);
  }
}

static int
unify_enum_key(PlTerm t, const enum_state *state)
{ if ( state->type == ENUM_PREFIX )
  { Slice k(state->it->key());

    if ( k.size_ >= state->prefix.length &&
	 memcmp(k.data_, state->prefix.string, state->prefix.length) == 0 )
    { k.data_ += state->prefix.length;
      k.size_ -= state->prefix.length;

      return unify(t, k, state->ref->type.key);
    } else
      return FALSE;
  } else
  { return unify(t, state->it->key(), state->ref->type.key);
  }
}


static int
enum_key_prefix(const enum_state *state)
{ if ( state->type == ENUM_PREFIX )
  { Slice k(state->it->key());
    return ( k.size_ >= state->prefix.length &&
	     memcmp(k.data_, state->prefix.string, state->prefix.length) == 0 );
  } else
    return TRUE;
}


static foreign_t
rocks_enum(PlTermv PL_av, int ac, enum_type type, control_t handle, ReadOptions options)
{ enum_state state_buf = {0};
  enum_state *state = &state_buf;

  switch(PL_foreign_control(handle))
  { case PL_FIRST_CALL:
      get_rocks(A1, &state->ref);
      if ( ac >= 4 )
      { char *prefix;
	size_t len;

	if ( !(state->ref->type.key == BLOB_ATOM ||
	       state->ref->type.key == BLOB_STRING ||
	       state->ref->type.key == BLOB_BINARY) )
	  return PL_permission_error("enum", "rocksdb", A1);

	if ( !PL_get_nchars(A4, &len, &prefix, REP_UTF8|CVT_IN|CVT_EXCEPTION) )
	  return FALSE;

	if ( type == ENUM_PREFIX )
	{ state->prefix.length = len;
	  state->prefix.string = prefix;
	}
	state->it = state->ref->db->NewIterator(options);
	state->it->Seek(prefix);
      } else
      { state->it = state->ref->db->NewIterator(options);
	state->it->SeekToFirst();
      }
      state->type = type;
      state->saved = FALSE;
      goto next;
    case PL_REDO:
      state = static_cast<enum_state *>(PL_foreign_context_address(handle));
    next:
    { PlFrame fr;
      for(; state->it->Valid(); state->it->Next())
      { if ( unify_enum_key(A2, state) &&
	     unify_value(A3, state->it->value(),
			 state->ref->builtin_merger, state->ref->type.value) )
	{ state->it->Next();
	  if ( state->it->Valid() && enum_key_prefix(state) )
	  { PL_retry_address(save_enum_state(state));
	  } else
	  { free_enum_state(state);
	    return TRUE;
	  }
	}
	fr.rewind();
      }
      free_enum_state(state);
      return FALSE;
    }
    case PL_PRUNED:
      state = static_cast<enum_state *>(PL_foreign_context_address(handle));
      free_enum_state(state);
      return TRUE;
    default:
      assert(0);
      return FALSE;
  }
  PL_fail;
}

PREDICATE_NONDET(rocks_enum, 4)
{ return rocks_enum(PL_av, 3, ENUM_ALL, handle, read_options(A4));
}

PREDICATE_NONDET(rocks_enum_from, 5)
{ return rocks_enum(PL_av, 4, ENUM_FROM, handle, read_options(A5));
}

PREDICATE_NONDET(rocks_enum_prefix, 5)
{ return rocks_enum(PL_av, 4, ENUM_PREFIX, handle, read_options(A5));
}

static PlAtom ATOM_delete("delete");
static PlAtom ATOM_put("put");

static void
batch_operation(const dbref *ref, WriteBatch &batch, PlTerm e)
{ atom_t name;
  size_t arity;

  if ( PL_get_name_arity(e, &name, &arity) )
  { if ( ATOM_delete == name && arity == 1 )
    { PlSlice key;

      get_slice(e[1], key, ref->type.key);
      batch.Delete(key);
    } else if ( ATOM_put == name && arity == 2 )
    { PlSlice key, value;

      get_slice(e[1], key,   ref->type.key);
      get_slice(e[2], value, ref->type.value);
      batch.Put(key, value);
    } else
    { throw PlDomainError("rocks_batch_operation", e);
    }
  } else
  { throw PlTypeError("compound", e);
  }
}


PREDICATE(rocks_batch, 3)
{ dbref *ref;

  get_rocks(A1, &ref);
  WriteBatch batch;
  PlTail tail(A2);
  PlTerm e;

  while(tail.next(e))
  { batch_operation(ref, batch, e);
  }

  return ok(ref->db->Write(write_options(A3), &batch));
}


static PlAtom ATOM_estimate_num_keys("estimate_num_keys");

PREDICATE(rocks_property, 3)
{ dbref *ref;
  atom_t prop;

  get_rocks(A1, &ref);

  if ( PL_get_atom(A2, &prop) )
  { if ( ATOM_estimate_num_keys == prop )
    { uint64_t value;

      return ( ref->db->GetIntProperty("rocksdb.estimate-num-keys", &value) &&
	       PL_unify_int64(A3, value) );
    } else
      throw PlDomainError("rocks_property", A2);
  }

  throw PlTypeError("atom", A2);
}
