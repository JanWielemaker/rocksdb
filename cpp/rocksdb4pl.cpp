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
#include <rocksdb/statistics.h>
#define PL_ARITY_AS_SIZE 1
#include <SWI-Stream.h>
#include <SWI-Prolog.h>
#include <SWI-cpp2.h>

#include <SWI-cpp2.cpp> // TODO: this should possibly be separate

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

struct dbref
{
  dbref()
    : db(             nullptr),
      symbol(         PlAtom(PlAtom::null)),
      name(           PlAtom(PlAtom::null)),
      flags(          0),
      builtin_merger( MERGE_NONE),
      merger(         PlRecord(PlRecord::null)),
      type(           { .key   = BLOB_ATOM,
                        .value = BLOB_ATOM})
  { }

  rocksdb::DB	*db;			/* DB handle */
  PlAtom         symbol;		/* associated symbol */
  PlAtom	 name;			/* alias name */
  int	         flags;			/* flags */
  merger_t	 builtin_merger;	/* C++ Merger */
  PlRecord	 merger;		/* merge option */
  struct
  { blob_type key;
    blob_type value;
  } type;
};


		 /*******************************
		 *	      ALIAS		*
		 *******************************/

std::mutex rocksdb4pl_alias_lock; // global
// TODO: Define the necessary operators for PlAtom, so that it can be
//       the key instead of atom_t.
static std::map<atom_t, PlAtom> alias_entries;

// rocks_get_alias() assumes that rocksdb4pl_alias_lock has been acquired
[[nodiscard]]
static PlAtom
rocks_get_alias(PlAtom name)
{ auto lookup = alias_entries.find(name.C_);
  if ( lookup == alias_entries.end() )
    return PlAtom(PlAtom::null);
  else
    return lookup->second;
}

[[nodiscard]]
static PlAtom
rocks_get_alias_locked(PlAtom name)
{ std::lock_guard<std::mutex> alias_lock_(rocksdb4pl_alias_lock);
  return rocks_get_alias(name);
}

static void
rocks_alias(PlAtom name, PlAtom symbol)
{ std::lock_guard<std::mutex> alias_lock_(rocksdb4pl_alias_lock);
  if ( rocks_get_alias(name).is_null() )
  { alias_entries.insert(std::make_pair(name.C_, symbol));
    name.register_ref();
    symbol.register_ref();
  } else
  { throw PlPermissionError("alias", "rocksdb", PlTerm_atom(name));
  }
}

static void
rocks_unalias(PlAtom name)
{ std::lock_guard<std::mutex> alias_lock_(rocksdb4pl_alias_lock);
  auto lookup = alias_entries.find(name.C_);
  if ( lookup == alias_entries.end() )
    return;
  name.unregister_ref();
  lookup->second.unregister_ref();  // alias_entries[name].unregister_ref()
  alias_entries.erase(lookup);
}


		 /*******************************
		 *	 SYMBOL REFERENCES	*
		 *******************************/

[[nodiscard]]
static bool
write_rocks_ref_(IOSTREAM *s, PlAtom eref, int flags)
{ auto refp = static_cast<dbref **>(eref.blob_data(nullptr, nullptr));
  auto ref  = *refp;

  Sfprintf(s, "<rocksdb>(%p)", ref);
  if ( flags&PL_WRT_NEWLINE )
    Sfprintf(s, "\n");
  return true;
}

[[nodiscard]]
static int // TODO: bool
write_rocks_ref(IOSTREAM *s, atom_t eref, int flags)
{ return write_rocks_ref_(s, PlAtom(eref), flags);
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
GC a rocks dbref blob from the atom garbage collector.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

[[nodiscard]]
static bool
release_rocks_ref_(PlAtom aref)
{ auto refp = static_cast<dbref **>(aref.blob_data(nullptr, nullptr));
  auto ref  = *refp;

  assert(ref->name.is_null());

  { auto db = ref->db;
    if ( db )
    { ref->db = nullptr;
      delete db;
    }
  }
  ref->merger.erase();
  delete ref;

  return true;
}

[[nodiscard]]
static int // TODO: bool
release_rocks_ref(atom_t aref)
{ return release_rocks_ref_(PlAtom(aref));
}

[[nodiscard]]
static bool
save_rocks_(PlAtom aref, IOSTREAM *fd)
{ auto refp = static_cast<dbref **>(aref.blob_data(nullptr, nullptr));
  auto ref  = *refp;
  (void)fd;

  return PL_warning("Cannot save reference to <rocksdb>(%p)", ref);
}

[[nodiscard]]
static int // TODO: bool
save_rocks(atom_t aref, IOSTREAM *fd)
{ return save_rocks_(PlAtom(aref), fd);
}

[[nodiscard]]
static PlAtom
load_rocks_(IOSTREAM *fd)
{ (void)fd;

  return PlAtom("<saved-rocksdb-ref>");
}

static atom_t
load_rocks(IOSTREAM *fd)
{ return load_rocks_(fd).C_;
}

static PL_blob_t rocks_blob =
{ .magic   = PL_BLOB_MAGIC,
  .flags   = PL_BLOB_UNIQUE,
  .name    = "rocksdb",
  .release = release_rocks_ref,
  .compare = nullptr,
  .write   = write_rocks_ref,
  .acquire = nullptr,
  .save    = save_rocks, // TODO: implement
  .load    = load_rocks  // TODO: implement
};


[[nodiscard]]
static bool
unify_rocks(PlTerm t, dbref *ref)
{ if ( ref->name.not_null() )
  { if ( ref->symbol.is_null() )
    { PlTerm_var tmp;
      PlCheckFail(tmp.unify_blob(&ref, sizeof ref, &rocks_blob));
      ref->symbol = t.as_atom();
      rocks_alias(ref->name, ref->symbol);
    }
    return t.unify_atom(ref->name);
  } else if ( ref->symbol.not_null() )
  { return t.unify_atom(ref->symbol);
  } else return ( t.unify_blob(&ref, sizeof ref, &rocks_blob) &&
		  t.get_atom(&ref->symbol) );
}


[[nodiscard]]
static dbref *
symbol_dbref(PlAtom symbol)
{ void *data;
  size_t len;
  PL_blob_t *type;

  if ( (data=symbol.blob_data(&len, &type)) && type == &rocks_blob )
  { auto erd = static_cast<dbref **>(data);
    return *erd;
  }

  return static_cast<dbref *>(nullptr);
}


[[nodiscard]]
static dbref *
get_rocks(PlTerm t, bool warn=true)
{ PlAtom a(PlAtom::null);

  if ( warn )
    a = t.as_atom();
  else
    t.get_atom_ex(&a);
  if ( t.not_null() )
  { for ( int i=0; i<2 && a.not_null(); i++ )
    { dbref *ref = symbol_dbref(a);

      if ( ref )
      { if ( !(ref->flags & DB_DESTROYED) )
	{ return ref;
	} else if ( warn )
	{ throw PlExistenceError("rocksdb", t);
	}
      }

      a = rocks_get_alias_locked(a);
    }

    throw PlExistenceError("rocksdb", t);
  }

  throw PlExistenceError("rocksdb", t);
}


		 /*******************************
		 *	      UTIL		*
		 *******************************/

PlException RocksError(const rocksdb::Status &status)
{ return PlGeneralError(PlCompound("rocks_error",
				   PlTermv(PlTerm_atom(status.ToString()))));
}


static bool
ok(const rocksdb::Status &status)
{ if ( status.ok() )
    return true;
  if ( status.IsNotFound() )
    return false;
  throw RocksError(status);
}

static void
ok_or_throw_fail(const rocksdb::Status &status)
{ PlCheckFail(ok(status));
}

class PlSlice
{
public:
  explicit PlSlice()
    : slice_() { }
  explicit PlSlice(const char *d, size_t n)
    : slice_(d, n) { }
  explicit PlSlice(const std::string& s)
    : slice_(s) { }

  const char* data() const { return slice_.data(); }
  size_t size() const { return slice_.size(); }
  std::string ToString(bool hex = false) const { return slice_.ToString(hex); }
  const rocksdb::Slice& slice() const { return slice_; }

  virtual ~PlSlice() = default;

protected:
  rocksdb::Slice slice_;
};

template<typename T>
class PlSliceNumber : public PlSlice
{
public:
  explicit PlSliceNumber<T>(T v)
    : PlSlice(reinterpret_cast<const char *>(&v_), sizeof v_),
      v_(v) { }

  virtual ~PlSliceNumber<T>() = default;

protected:
  T v_; // backing store for rocksdb::slice
};

class PlSliceStr : public PlSlice
{
public:
  explicit PlSliceStr(const std::string& s)
    : v_(s)
  { // Assign slice after setting up v_ because Slice(v_) uses both
    // the address of v_ and the size; if we used the initializer
    // list, the fields are done in the order of their declaration.
    slice_ = rocksdb::Slice(v_);
  }

  virtual ~PlSliceStr() = default;

protected:
  std::string v_; // backing store for rocksdb::slice
};


#define CVT_IN	(CVT_ATOM|CVT_STRING|CVT_LIST)

[[nodiscard]]
static std::unique_ptr<PlSlice>
get_slice(PlTerm t, blob_type type)
{ switch ( type )
  { case BLOB_ATOM:
    case BLOB_STRING:
      return std::make_unique<PlSliceStr>(t.get_nchars(CVT_IN|CVT_EXCEPTION|REP_UTF8));
    case BLOB_BINARY:
      return std::make_unique<PlSliceStr>(t.get_nchars(CVT_IN|CVT_EXCEPTION));
    case BLOB_INT32:
      { int32_t v;
        t.integer(&v);
        return std::make_unique<PlSliceNumber<int32_t>>(v);
      }
    case BLOB_INT64:
      { int64_t v;
        t.integer(&v);
        return std::make_unique<PlSliceNumber<int64_t>>(v);
      }
    case BLOB_FLOAT32:
      return std::make_unique<PlSliceNumber<float>>(static_cast<float>(t.as_float()));
    case BLOB_FLOAT64:
      return std::make_unique<PlSliceNumber<double>>(t.as_double());
    case BLOB_TERM:
      { PlRecordExternalCopy e(t); // declared here so that it stays in scope for return
        return std::make_unique<PlSliceStr>(e.data());
      }
    default:
      assert(0);
  }
}


[[nodiscard]]
static bool
unify(PlTerm t, const rocksdb::Slice &s, blob_type type)
{ switch ( type )
  { case BLOB_ATOM:
      return t.unify_chars(PL_ATOM|REP_UTF8, s.size_, s.data_);
    case BLOB_STRING:
      return t.unify_chars(PL_STRING|REP_UTF8, s.size_, s.data_);
    case BLOB_BINARY:
      return t.unify_chars(PL_STRING|REP_ISO_LATIN_1, s.size_, s.data_);
    case BLOB_INT32:
    { int i;
      memcpy(&i, s.data_, sizeof i); // Unaligned i=*reinterpret_cast<int>(s.data_)
      return t.unify_integer(i);
    }
    case BLOB_INT64:
    { int64_t i;
      memcpy(&i, s.data_, sizeof i);
      return t.unify_integer(i);
    }
    case BLOB_FLOAT32:
    { float f;
      memcpy(&f, s.data_, sizeof f);
      return t.unify_float(f);
    }
    case BLOB_FLOAT64:
    { double f;
      memcpy(&f, s.data_, sizeof f);
      return t.unify_float(f);
    }
    case BLOB_TERM:
    { PlTerm_var tmp;
      Plx_recorded_external(s.data_, tmp.C_);
      return tmp.unify_term(t);
    }
    default:
      assert(0);
      return false;
  }
}

[[nodiscard]]
static bool
unify(PlTerm t, const rocksdb::Slice *s, blob_type type)
{ if ( s )
    return unify(t, *s, type);

  switch ( type )
  { case BLOB_ATOM:
    { static const PlAtom ATOM_("");
      return t.unify_atom(ATOM_);
    }
    case BLOB_STRING:
    case BLOB_BINARY:
      return t.unify_chars(PL_STRING, 0, "");
    case BLOB_INT32:
    case BLOB_INT64:
      return t.unify_integer(0);
    case BLOB_FLOAT32:
    case BLOB_FLOAT64:
      return t.unify_float(0.0);
    case BLOB_TERM:
      return t.unify_nil();
    default:
      assert(0);
      return false;
  }
}

[[nodiscard]]
static bool
unify(PlTerm t, const std::string &s, blob_type type)
{ rocksdb::Slice sl(s);

  return unify(t, sl, type);
}

[[nodiscard]]
static bool
unify_value(PlTerm t, const rocksdb::Slice &s, merger_t merge, blob_type type)
{ if ( merge == MERGE_NONE )
   return unify(t, s, type);

  PlTerm_tail list(t);
  PlTerm_var tmp;
  const char *data = s.data();
  const char *end  = data+s.size();

  while ( data < end )
  { switch ( type )
    { case BLOB_INT32:
      { int i;
	memcpy(&i, data, sizeof i);
	data += sizeof i;
	Plx_put_integer(tmp.C_, i);
      }
      break;
      case BLOB_INT64:
      { int64_t i;
	memcpy(&i, data, sizeof i);
	data += sizeof i;
	Plx_put_int64(tmp.C_, i);
      }
      break;
      case BLOB_FLOAT32:
      { float i;
	memcpy(&i, data, sizeof i);
	data += sizeof i;
	Plx_put_float(tmp.C_, i);
      }
      break;
      case BLOB_FLOAT64:
      { double i;
	memcpy(&i, data, sizeof i);
	data += sizeof i;
	Plx_put_float(tmp.C_, i);
      }
      break;
      default:
	assert(0);
	return false;
    }

    if ( !list.append(tmp) )
      return false;
  }

  return list.close();
}

[[nodiscard]]
static bool
unify_value(PlTerm t, const std::string &s, merger_t merge, blob_type type)
{ rocksdb::Slice sl(s);

  return unify_value(t, sl, merge, type);
}


		 /*******************************
		 *	       MERGER		*
		 *******************************/

[[nodiscard]]
static bool
log_exception(rocksdb::Logger* logger)
{ PlTerm_term_t ex(Plx_exception(0));

  Log(logger, "%s", ex.as_string(PlEncoding::UTF8).c_str());
  return false; // For convenience, allowing: return log_exception(logger);
}

class engine
{
private:
  int tid = 0;

public:
  engine()
  { if ( Plx_thread_self() == -1 )
    { if ( (tid=Plx_thread_attach_engine(nullptr)) < 0 )
      { PlTerm_term_t ex(Plx_exception(0));
	if ( ex.not_null() )
	  throw PlException(ex);
	else
	  throw PlResourceError("memory");
      }
    }
  }

  ~engine()
  { if ( tid > 0 )
      Plx_thread_destroy_engine();
  }
};

[[nodiscard]]
static bool
call_merger(const dbref *ref, PlTermv av, std::string* new_value,
	    rocksdb::Logger* logger)
{ static PlPredicate pred_call6(PlFunctor("call", 6), PlModule("system"));

  try
  { PlQuery q(pred_call6, av);
    if ( q.next_solution() )
    { auto answer = get_slice(av[5], ref->type.value);
      new_value->assign(answer->data(), answer->size());
      return true;
    } else
    { Log(logger, "merger failed");
      return false;
    }
  } catch(PlException &ex)
  { Log(logger, "%s", ex.as_string(PlEncoding::UTF8).c_str());
    return false;
  }
}


class PrologMergeOperator : public rocksdb::MergeOperator
{
private:
  const dbref *ref;

public:
  explicit PrologMergeOperator(const dbref *reference)
    : ref(reference) { }

  virtual bool
  FullMerge(const rocksdb::Slice& key,
	    const rocksdb::Slice* existing_value,
	    const std::deque<std::string>& operand_list,
	    std::string* new_value,
	    rocksdb::Logger* logger) const override
  { engine e;
    PlTermv av(6);
    PlTerm_tail list(av[4]);
    PlTerm_var tmp;
    static const PlAtom ATOM_full("full");

    for (const auto& value : operand_list)
    { Plx_put_variable(tmp.C_);
      if ( !unify(tmp, value, ref->type.value) ||
           !list.append(tmp) )
        return false;
    }
    if ( !list.close() )
      return false;

    if ( av[0].unify_term(ref->merger.term()) &&
	 av[1].unify_atom(ATOM_full) &&
	 unify(av[2], key, ref->type.key) &&
	 unify(av[3], existing_value, ref->type.value) )
      return call_merger(ref, av, new_value, logger);
    else
      return log_exception(logger);
  }

  virtual bool
  PartialMerge(const rocksdb::Slice& key,
	       const rocksdb::Slice& left_operand,
	       const rocksdb::Slice& right_operand,
	       std::string* new_value,
	       rocksdb::Logger* logger) const override
  { engine e;
    PlTermv av(6);
    static const PlAtom ATOM_partial("partial");

    if ( av[0].unify_term(ref->merger.term()) &&
	 av[1].unify_atom(ATOM_partial) &&
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

template<typename Number_t>
[[nodiscard]]
static int
cmp_number(const void *v1, const void *v2)
{ auto i1 = static_cast<const Number_t *>(v1);
  auto i2 = static_cast<const Number_t *>(v2);

  return *i1 > *i2 ? 1 : *i1 < *i2 ? -1 : 0;
}

template<typename Number_t>
static void
sort_numbers(std::string *str)
{ auto s = str->data();
  auto len = str->length();
  if ( len == 0 )
    return;
  auto ip = reinterpret_cast<Number_t *>(s);
  auto op = ip+1;
  auto ep = reinterpret_cast<Number_t *>(s+len);
  Number_t cv;

  qsort(s, len / sizeof ip, sizeof ip, cmp_number<Number_t>);
  cv = *ip;
  for ( ip++; ip < ep; ip++ )
  { if ( *ip != cv )
      *op++ = cv = *ip;
  }
  str->resize(static_cast<size_t>(reinterpret_cast<char *>(op) - s));
}


void
sort(std::string *str, blob_type type)
{ switch ( type )
  { case BLOB_INT32:
      sort_numbers<int32_t>(str);
      break;
    case BLOB_INT64:
      sort_numbers<int64_t>(str);
      break;
    case BLOB_FLOAT32:
      sort_numbers<float>(str);
      break;
    case BLOB_FLOAT64:
      sort_numbers<double>(str);
      break;
    default:
      assert(0);
      break;
  }
}


class ListMergeOperator : public rocksdb::MergeOperator
{
private:
  const dbref *ref;

public:
  explicit ListMergeOperator(const dbref *reference)
    : ref(reference) { }

  virtual bool
  FullMerge(const rocksdb::Slice& key,
	    const rocksdb::Slice* existing_value,
	    const std::deque<std::string>& operand_list,
	    std::string* new_value,
	    rocksdb::Logger* logger) const override
  { (void)key;
    (void)logger;
    std::string s(existing_value ? existing_value->ToString() : "");

    for (const auto& value : operand_list)
    { s += value;
    }

    if ( ref->builtin_merger == MERGE_SET )
      sort(&s, ref->type.value);
    *new_value = s;
    return true;
  }

  virtual bool
  PartialMerge(const rocksdb::Slice& key,
	       const rocksdb::Slice& left_operand,
	       const rocksdb::Slice& right_operand,
	       std::string* new_value,
	       rocksdb::Logger* logger) const override
  { (void)key;
    (void)logger;
    std::string s(left_operand.ToString());
    s += right_operand.ToString();

    if ( ref->builtin_merger == MERGE_SET )
      sort(&s, ref->type.value);
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

static void
get_blob_type(PlTerm t, blob_type *key_type, merger_t *m)
{ PlAtom a(PlAtom::null);

  static const PlFunctor FUNCTOR_list1("list", 1);
  static const PlFunctor FUNCTOR_set1("set", 1);
  static const PlAtom ATOM_atom("atom");
  static const PlAtom ATOM_string("string");
  static const PlAtom ATOM_binary("binary");
  static const PlAtom ATOM_term("term");
  static const PlAtom ATOM_int32("int32");
  static const PlAtom ATOM_int64("int64");
  static const PlAtom ATOM_float("float");
  static const PlAtom ATOM_double("double");

  if ( m && t.is_functor(FUNCTOR_list1) )
  { *m = MERGE_LIST;
    t[1].get_atom_ex(&a);
  } else if ( m && t.is_functor(FUNCTOR_set1) )
  { *m = MERGE_SET;
    t[1].get_atom_ex(&a);
  } else
    t.get_atom_ex(&a);

  if ( !m || *m == MERGE_NONE )
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
}


typedef void (*ReadOptdefAction)(rocksdb::ReadOptions *options, PlTerm t);

struct ReadOptdef
{ const char *name;
  ReadOptdefAction action;
  PlAtom atom; // Initially PlAtom::null; filled in as-needed by lookup
};

#define RD_ODEF [](rocksdb::ReadOptions *options, PlTerm arg)

// TODO: move read_optdefs into read_options(), remove the PlAtom(PlAtom::null),
//       replacing ReadOptdef::name with PlAtom ... C++ will initialize the
//       "static" const structure on first use.
static ReadOptdef read_optdefs[] =
{ // "snapshot" const Snapshot*
  // "iterate_lower_bound" const rocksdb::Slice*
  // "iterate_upper_bound" const rocksdb::Slice*
  {         "readahead_size",                       RD_ODEF {
    options->readahead_size                       = arg.as_size_t(); }, PlAtom(PlAtom::null) },
  {         "max_skippable_internal_keys",          RD_ODEF {
    options->max_skippable_internal_keys          = arg.as_uint64_t(); }, PlAtom(PlAtom::null) },
  // "read_tier" ReadTier
  {         "verify_checksums",                     RD_ODEF {
    options->verify_checksums                     = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "fill_cache",                           RD_ODEF {
    options->fill_cache                           = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "tailing",                              RD_ODEF {
    options->tailing                              = arg.as_bool(); }, PlAtom(PlAtom::null) },
  // "managed" - not used any more
  {         "total_order_seek",                     RD_ODEF {
    options->total_order_seek                     = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "auto_prefix_mode",                     RD_ODEF {
    options->auto_prefix_mode                     = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "prefix_same_as_start",                 RD_ODEF {
    options->prefix_same_as_start                 = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "pin_data",                             RD_ODEF {
    options->pin_data                             = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "background_purge_on_iterator_cleanup", RD_ODEF {
    options->background_purge_on_iterator_cleanup = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "ignore_range_deletions",               RD_ODEF {
    options->ignore_range_deletions               = arg.as_bool(); }, PlAtom(PlAtom::null) },
  // "table_filter" std::function<bool(const TableProperties&)>
  // TODO: "iter_start_seqnum" removed from rocksdb/include/options.h?
  // {         "iter_start_seqnum",                    RD_ODEF {
  //   options->iter_start_seqnum                    = static_cast<SequenceNumber>(arg); }, PlAtom(PlAtom::null) },
  //         "timestamp" rocksdb::Slice*
  // "iter_start_ts" rocksdb::Slice*
  {
	     "deadline",                            RD_ODEF {
      options->deadline                             = static_cast<std::chrono::microseconds>(arg.as_int64_t()); }, PlAtom(PlAtom::null) },
  {         "io_timeout",                           RD_ODEF {
      options->io_timeout                           = static_cast<std::chrono::microseconds>(arg.as_int64_t()); }, PlAtom(PlAtom::null) },
  {         "value_size_soft_limit",                RD_ODEF {
      options->value_size_soft_limit                = arg.as_uint64_t(); }, PlAtom(PlAtom::null) },
  { nullptr, nullptr, PlAtom(PlAtom::null) }
};


// TODO: consolidate lookup_read_optdef_and_apply,
//       lookup_write_optdef_and_apply, lookup_optdef_and_apply;
//       possibly using STL map (although that doesn't support
//       lazy filling in of the atoms in the lookup table)
static void
lookup_read_optdef_and_apply(rocksdb::ReadOptions *options,
			     ReadOptdef opt_defs[],
			     PlAtom name, PlTerm opt)
{ for ( auto def=opt_defs; def->name; def++ )
  { if ( def->atom.is_null() ) // lazily fill in atoms in lookup table
      def->atom = PlAtom(def->name);
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
  PlAtom atom; // Initially PlAtom::null; filled in as-needed by lookup
};

#define WR_ODEF [](rocksdb::WriteOptions *options, PlTerm arg)

// TODO: move write_optdefs into write_options(), remove the PlAtom(PlAtom::null),
//       replacing WriteOptdef::name with PlAtom ... C++ will initialize the
//       "static" const structure on first use.
static WriteOptdef write_optdefs[] =
{ {         "sync",                           WR_ODEF {
    options->sync                           = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "disableWAL",                     WR_ODEF {
    options->disableWAL                     = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "ignore_missing_column_families", WR_ODEF {
    options->ignore_missing_column_families = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "no_slowdown",                    WR_ODEF {
    options->no_slowdown                    = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "low_pri",                        WR_ODEF {
    options->low_pri                        = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "memtable_insert_hint_per_batch", WR_ODEF {
    options->memtable_insert_hint_per_batch = arg.as_bool(); }, PlAtom(PlAtom::null) },
  // "timestamp" rocksdb::Slice*

  { nullptr, nullptr, PlAtom(PlAtom::null) }
};


static void
lookup_write_optdef_and_apply(rocksdb::WriteOptions *options,
			      WriteOptdef opt_defs[],
			      PlAtom name, PlTerm opt)
{ for ( auto def=opt_defs; def->name; def++ )
  { if ( def->atom.is_null() ) // lazilly fill in atoms in lookup table
      def->atom = PlAtom(def->name);
    if ( def->atom == name )
    { def->action(options, opt[1]);
      return;
    }
  }
  throw PlTypeError("option", opt);
}

static void
options_set_InfoLogLevel(rocksdb::Options *options, PlTerm arg)
{ rocksdb::InfoLogLevel log_level;

  static const PlAtom ATOM_debug("debug");
  static const PlAtom ATOM_info("info");
  static const PlAtom ATOM_warn("warn");
  static const PlAtom ATOM_error("error");
  static const PlAtom ATOM_fatal("fatal");
  static const PlAtom ATOM_header("header");

  const auto arg_a = arg.as_atom();
       if ( arg_a == ATOM_debug  ) log_level = rocksdb::DEBUG_LEVEL;
  else if ( arg_a == ATOM_info   ) log_level = rocksdb::INFO_LEVEL;
  else if ( arg_a == ATOM_warn   ) log_level = rocksdb::WARN_LEVEL;
  else if ( arg_a == ATOM_error  ) log_level = rocksdb::ERROR_LEVEL;
  else if ( arg_a == ATOM_fatal  ) log_level = rocksdb::FATAL_LEVEL;
  else if ( arg_a == ATOM_header ) log_level = rocksdb::HEADER_LEVEL;
  else throw PlTypeError("InfoLogLevel", arg); // TODO: this causes SIGSEGV
  options->info_log_level = log_level;
}


typedef void (*OptdefAction)(rocksdb::Options *options, PlTerm t);

struct Optdef
{ const char *name;
  OptdefAction action;
  PlAtom atom; // Initially PlAtom::null; filled in as-needed by lookup
};

#define ODEF [](rocksdb::Options *options, PlTerm arg)

static Optdef optdefs[] =
{ { "prepare_for_bulk_load",                           ODEF { if ( arg.as_bool() ) options->PrepareForBulkLoad(); }, PlAtom(PlAtom::null) }, // TODO: what to do with false?
  { "optimize_for_small_db",                           ODEF { if ( arg.as_bool() ) options->OptimizeForSmallDb(); }, PlAtom(PlAtom::null) }, // TODO: what to do with false? - there's no DontOptimizeForSmallDb()
#ifndef ROCKSDB_LITE
  { "increase_parallelism",                            ODEF { if ( arg.as_bool() ) options->IncreaseParallelism(); }, PlAtom(PlAtom::null) },
#endif
  {         "create_if_missing",                       ODEF {
    options->create_if_missing                       = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "create_missing_column_families",          ODEF {
    options->create_missing_column_families          = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "error_if_exists",                         ODEF {
    options->error_if_exists                         = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "paranoid_checks",                         ODEF {
    options->paranoid_checks                         = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "track_and_verify_wals_in_manifest",       ODEF {
    options->track_and_verify_wals_in_manifest       = arg.as_bool(); }, PlAtom(PlAtom::null) },
  // "env" Env::Default
  // "rate_limiter" - shared_ptr<RateLimiter>
  // "sst_file_manager" - shared_ptr<SstFileManager>
  // "info_log" - shared_ptr<rocksdb::Logger> - see comment in ../README.md
  { "info_log_level",                                  options_set_InfoLogLevel, PlAtom(PlAtom::null) },
  {         "max_open_files",                          ODEF {
    options->max_open_files                          = arg.as_int(); }, PlAtom(PlAtom::null) },
  {         "max_file_opening_threads",                ODEF {
    options-> max_file_opening_threads               = arg.as_int(); }, PlAtom(PlAtom::null) },
  {         "max_total_wal_size",                      ODEF {
    options->max_total_wal_size                      = arg.as_uint64_t(); }, PlAtom(PlAtom::null) },
  {         "statistics",                              ODEF {
    options->statistics = arg.as_bool() ? rocksdb::CreateDBStatistics() : nullptr; }, PlAtom(PlAtom::null) },
  {         "use_fsync",                               ODEF {
    options->use_fsync                               = arg.as_bool(); }, PlAtom(PlAtom::null) },
  // "db_paths" - vector<DbPath>
  {         "db_log_dir",                              ODEF {
    options->db_log_dir                              = arg.as_string(PlEncoding::Locale); }, PlAtom(PlAtom::null) },
  {         "wal_dir",                                 ODEF {
    options->wal_dir                                 = arg.as_string(PlEncoding::Locale); }, PlAtom(PlAtom::null) },
  {         "delete_obsolete_files_period_micros",     ODEF {
    options->delete_obsolete_files_period_micros     = arg.as_uint64_t(); }, PlAtom(PlAtom::null) },
  {         "max_background_jobs",                     ODEF {
    options->max_background_jobs                     = arg.as_int(); }, PlAtom(PlAtom::null) },
  // "base_background_compactions" is obsolete
  // "max_background_compactions" is obsolete
  {         "max_subcompactions",                      ODEF {
    options->max_subcompactions                      = arg.as_uint32_t(); }, PlAtom(PlAtom::null) },
  // "max_background_flushes" is obsolete
  {         "max_log_file_size",                       ODEF {
    options->max_log_file_size                       = arg.as_size_t(); }, PlAtom(PlAtom::null) },
  {         "log_file_time_to_roll",                   ODEF {
    options->log_file_time_to_roll                   = arg.as_size_t(); }, PlAtom(PlAtom::null) },
  {         "keep_log_file_num",                       ODEF {
    options->keep_log_file_num                       = arg.as_size_t(); }, PlAtom(PlAtom::null) },
  {         "recycle_log_file_num",                    ODEF {
    options->recycle_log_file_num                    = arg.as_size_t(); }, PlAtom(PlAtom::null) },
  {         "max_manifest_file_size",                  ODEF {
    options->max_manifest_file_size                  = arg.as_uint64_t(); }, PlAtom(PlAtom::null) },
  {         "table_cache_numshardbits",                ODEF {
    options->table_cache_numshardbits                = arg.as_int(); }, PlAtom(PlAtom::null) },
  {         "wal_ttl_seconds",                         ODEF {
    options->WAL_ttl_seconds                         = arg.as_uint64_t(); }, PlAtom(PlAtom::null) },
  {         "wal_size_limit_mb",                       ODEF {
    options->WAL_size_limit_MB                       = arg.as_uint64_t(); }, PlAtom(PlAtom::null) },
  {         "manifest_preallocation_size",             ODEF {
    options->manifest_preallocation_size             = arg.as_size_t(); }, PlAtom(PlAtom::null) },
  {         "allow_mmap_reads",                        ODEF {
    options->allow_mmap_reads                        = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "allow_mmap_writes",                       ODEF {
    options->allow_mmap_writes                       = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "use_direct_reads",                        ODEF {
    options->use_direct_reads                        = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "use_direct_io_for_flush_and_compaction",  ODEF {
    options->use_direct_io_for_flush_and_compaction  = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "allow_fallocate",                         ODEF {
    options->allow_fallocate                         = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "is_fd_close_on_exec",                     ODEF {
    options->is_fd_close_on_exec                     = arg.as_bool(); }, PlAtom(PlAtom::null) },
  // "skip_log_error_on_recovery" is obsolete
  {         "stats_dump_period_sec",                   ODEF {
    options->stats_dump_period_sec                   = arg.as_uint32_t(); }, PlAtom(PlAtom::null) }, // TODO: match: unsigned int stats_dump_period_sec
  {         "stats_persist_period_sec",                ODEF {
      options->stats_persist_period_sec                = arg.as_uint32_t(); }, PlAtom(PlAtom::null) }, // TODO: match: unsigned int stats_persist_period_sec
  {         "persist_stats_to_disk",                   ODEF {
    options->persist_stats_to_disk                   = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "stats_history_buffer_size",               ODEF {
    options->stats_history_buffer_size               = arg.as_size_t(); }, PlAtom(PlAtom::null) },
  {         "advise_random_on_open",                   ODEF {
    options->advise_random_on_open                   = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "db_write_buffer_size",                    ODEF {
    options->db_write_buffer_size                    = arg.as_size_t(); }, PlAtom(PlAtom::null) },
  // "write_buffer_manager" - shared_ptr<WriteBufferManager>
  // "access_hint_on_compaction_start" - enum AccessHint
  // TODO: "new_table_reader_for_compaction_inputs"  removed from rocksdb/include/options.h?
  // {         "new_table_reader_for_compaction_inputs",  ODEF {
//   options->new_table_reader_for_compaction_inputs  = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "compaction_readahead_size",               ODEF {
    options->compaction_readahead_size               = arg.as_size_t(); }, PlAtom(PlAtom::null) },
  {         "random_access_max_buffer_size",           ODEF {
    options->random_access_max_buffer_size           = arg.as_size_t(); }, PlAtom(PlAtom::null) },
  {         "writable_file_max_buffer_size",           ODEF {
    options->writable_file_max_buffer_size           = arg.as_size_t(); }, PlAtom(PlAtom::null) },
  {         "use_adaptive_mutex",                      ODEF {
    options->use_adaptive_mutex                      = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "bytes_per_sync",                          ODEF {
    options->bytes_per_sync                          = arg.as_uint64_t(); }, PlAtom(PlAtom::null) },
  {         "wal_bytes_per_sync",                      ODEF {
    options->wal_bytes_per_sync                      = arg.as_uint64_t(); }, PlAtom(PlAtom::null) },
  {         "strict_bytes_per_sync",                   ODEF {
    options->strict_bytes_per_sync                   = arg.as_bool(); }, PlAtom(PlAtom::null) },
  // "listeners" - vector<shared_ptr<EventListener>>
  {         "enable_thread_tracking",                  ODEF {
    options->enable_thread_tracking                  = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "delayed_write_rate",                      ODEF {
    options->delayed_write_rate                      = arg.as_uint64_t(); }, PlAtom(PlAtom::null) },
  {         "enable_pipelined_write",                  ODEF {
    options->enable_pipelined_write                  = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "unordered_write",                         ODEF {
    options->unordered_write                         = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "allow_concurrent_memtable_write",         ODEF {
    options->allow_concurrent_memtable_write         = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "enable_write_thread_adaptive_yield",      ODEF {
    options->enable_write_thread_adaptive_yield      = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "max_write_batch_group_size_bytes",        ODEF {
    options->max_write_batch_group_size_bytes        = arg.as_uint64_t(); }, PlAtom(PlAtom::null) },
  {         "write_thread_max_yield_usec",             ODEF {
    options->write_thread_max_yield_usec             = arg.as_uint64_t(); }, PlAtom(PlAtom::null) },
  {         "write_thread_slow_yield_usec",            ODEF {
    options->write_thread_slow_yield_usec            = arg.as_uint64_t(); }, PlAtom(PlAtom::null) },
  {         "skip_stats_update_on_db_open",            ODEF {
    options->skip_stats_update_on_db_open            = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "skip_checking_sst_file_sizes_on_db_open", ODEF {
    options->skip_checking_sst_file_sizes_on_db_open = arg.as_bool(); }, PlAtom(PlAtom::null) },
  // "wal_recovery_mode" - enum WALRecoveryMode
  {         "allow_2pc",                               ODEF {
    options->allow_2pc                               = arg.as_bool(); }, PlAtom(PlAtom::null) },
  // "row_cache" - shared_ptr<Cache>
  // "wal_filter" - WalFilter*
  {         "fail_ifoptions_file_error",               ODEF {
    options->fail_if_options_file_error              = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "dump_malloc_stats",                       ODEF {
    options->dump_malloc_stats                       = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "avoid_flush_during_recovery",             ODEF {
    options->avoid_flush_during_recovery             = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "avoid_flush_during_shutdown",             ODEF {
    options->avoid_flush_during_shutdown             = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "allow_ingest_behind",                     ODEF {
    options->allow_ingest_behind                     = arg.as_bool(); }, PlAtom(PlAtom::null) },
  // TODO: "preserve_deletes" removed from rocksdb/include/options.h?
  // {         "preserve_deletes",                        ODEF {
//   options->preserve_deletes                        = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "two_write_queues",                        ODEF {
    options->two_write_queues                        = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "manual_wal_flush",                        ODEF {
    options->manual_wal_flush                        = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "atomic_flush",                            ODEF {
    options->atomic_flush                            = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "avoid_unnecessary_blocking_io",           ODEF {
    options->avoid_unnecessary_blocking_io           = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "write_dbid_to_manifest",                  ODEF {
    options->write_dbid_to_manifest                  = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "log_readahead_size",                      ODEF {
    options->write_dbid_to_manifest                  = arg.as_bool(); }, PlAtom(PlAtom::null) },
  // "file_checksum_gen_factory" - std::shared_ptr<FileChecksumGenFactory>
  { "best_efforts_recovery",                           ODEF {
    options->best_efforts_recovery                   = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "max_bgerror_resume_count",                ODEF {
    options->max_bgerror_resume_count                = arg.as_int(); }, PlAtom(PlAtom::null) },
  {         "bgerror_resume_retry_interval",           ODEF {
    options->bgerror_resume_retry_interval           = arg.as_uint64_t(); }, PlAtom(PlAtom::null) },
  {         "allow_data_in_errors",                    ODEF {
    options->allow_data_in_errors                    = arg.as_bool(); }, PlAtom(PlAtom::null) },
  {         "db_host_id",                              ODEF {
      options->db_host_id                            = arg.as_string(PlEncoding::Locale); }, PlAtom(PlAtom::null) },
  // "checksum_handoff_file_types" - FileTypeSet

  { nullptr, nullptr, PlAtom(PlAtom::null) }
};

// TODO: move optdefs into options(), remove the PlAtom(PlAtom::null),
//       replacing Optdef::name with PlAtom ... C++ will initialize the
//       "static" const structure on first use.

static void
lookup_optdef_and_apply(rocksdb::Options *options,
			Optdef opt_defs[],
			PlAtom name, PlTerm opt)
{ for ( auto def=opt_defs; def->name; def++ )
  { if ( def->atom.is_null() ) // lazilly fill in atoms in lookup table
      def->atom = PlAtom(def->name);
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
  PlAtom alias(PlAtom::null);
  PlRecord merger(PlRecord::null);
  int once = false;
  int read_only = false;

  static const PlAtom ATOM_key("key");
  static const PlAtom ATOM_value("value");
  static const PlAtom ATOM_alias("alias");
  static const PlAtom ATOM_merge("merge");
  static const PlAtom ATOM_open("open");
  static const PlAtom ATOM_once("once");
  static const PlAtom ATOM_mode("mode");
  static const PlAtom ATOM_read_write("read_write");
  static const PlAtom ATOM_read_only("read_only");

  PlCheckFail(Plx_get_file_name(A1.C_, &fn, PL_FILE_OSPATH));
  PlTerm_tail tail(A3);
  PlTerm_var opt;
  while ( tail.next(opt) )
  { PlAtom name(PlAtom::null);
    size_t arity;

    PlCheckFail(opt.name_arity(&name, &arity));
    if (  arity == 1 )
    { if ( ATOM_key == name )
	get_blob_type(opt[1], &key_type, static_cast<merger_t *>(nullptr));
      else if ( ATOM_value == name )
	get_blob_type(opt[1], &value_type, &builtin_merger);
      else if ( ATOM_merge == name )
	merger = opt[1].record();
      else if ( ATOM_alias == name )
      { alias = opt[1].as_atom();
	once = true;
      } else if ( ATOM_open == name )
      { if ( ATOM_once == opt[1].as_atom() )
	  once = true;
	else
	  throw PlDomainError("open_option", opt[1]);
      } else if ( ATOM_mode == name )
      { PlAtom a = opt[1].as_atom();
	if ( ATOM_read_write == a )
	  read_only = false;
	else if ( ATOM_read_only == a )
	  read_only = true;
	else
	  throw PlDomainError("mode_option", opt[1]);
      } else
      { lookup_optdef_and_apply(&options, optdefs, name, opt);
      }
    } else
      throw PlTypeError("option", opt);
  }

  if ( alias.not_null() && once )
  { PlAtom existing = rocks_get_alias_locked(alias);
    if ( existing.not_null() )
    { dbref *eref;
      if ( (eref=symbol_dbref(existing)) &&
	   (eref->flags&DB_OPEN_ONCE) )
	return A2.unify_atom(existing);
    }
  }

  auto ref = new dbref();
  ref->merger         = merger;
  ref->builtin_merger = builtin_merger;
  ref->type.key       = key_type;
  ref->type.value     = value_type;
  ref->name           = alias;
  if ( once )
    ref->flags |= DB_OPEN_ONCE;

  try
  { if ( ref->merger.not_null() )
      options.merge_operator.reset(new PrologMergeOperator(ref));
    else if ( builtin_merger != MERGE_NONE )
      options.merge_operator.reset(new ListMergeOperator(ref));
    if ( read_only )
      ok_or_throw_fail(rocksdb::DB::OpenForReadOnly(options, fn, &ref->db));
    else
      ok_or_throw_fail(rocksdb::DB::Open(options, fn, &ref->db));
    PlCheckFail(unify_rocks(A2, ref));
  } catch(...)
  { delete ref;
    throw;
  }
  return true;
}


PREDICATE(rocks_close, 1)
{ dbref *ref = get_rocks(A1);
  rocksdb::DB* db = ref->db;

  ref->db = nullptr;
  ref->flags |= DB_DESTROYED;
  if ( ref->name.not_null() )
  { rocks_unalias(ref->name);
    ref->name.reset();
  }

  delete db;
  return true;
}


[[nodiscard]]
static rocksdb::WriteOptions
write_options(PlTerm options_term)
{ rocksdb::WriteOptions options;
  PlTerm_tail tail(options_term);
  PlTerm_var opt;
  while ( tail.next(opt) )
  { PlAtom name(PlAtom::null);
    size_t arity;

    PlCheckFail(opt.name_arity(&name, &arity));
    if ( arity == 1 )
      lookup_write_optdef_and_apply(&options, write_optdefs, name, opt);
    else
      throw PlTypeError("option", opt);
  }
  return options;
}


PREDICATE(rocks_put, 4)
{ dbref *ref = get_rocks(A1);
  auto key = get_slice(A2, ref->type.key);

  if ( ref->builtin_merger == MERGE_NONE )
  { auto value = get_slice(A3, ref->type.value);
    ok_or_throw_fail(ref->db->Put(write_options(A4), key->slice(), value->slice()));
  } else
  { PlTerm_tail list(A3);
    PlTerm_var tmp;
    std::string value;

    while ( list.next(tmp) )
    { auto s = get_slice(tmp, ref->type.value);
      value += s->ToString();
    }

    if ( ref->builtin_merger == MERGE_SET )
      sort(&value, ref->type.value);

    ok_or_throw_fail(ref->db->Put(write_options(A4), key->slice(), value));
  }

  return true;
}

PREDICATE(rocks_merge, 4)
{ dbref *ref = get_rocks(A1);
  if ( ref->merger.is_null() && ref->builtin_merger == MERGE_NONE )
    throw PlPermissionError("merge", "rocksdb", A1);

  auto key = get_slice(A2,ref->type.key);
  auto value = get_slice(A3, ref->type.value);

  ok_or_throw_fail(ref->db->Merge(write_options(A4), key->slice(), value->slice()));

  return true;
}

[[nodiscard]]
static rocksdb::ReadOptions
read_options(PlTerm options_term)
{ rocksdb::ReadOptions options;
  PlTerm_tail tail(options_term);
  PlTerm_var opt;
  while ( tail.next(opt) )
  { PlAtom name(PlAtom::null);
    size_t arity;
    PlCheckFail(opt.name_arity(&name, &arity));
    if ( arity == 1 )
      lookup_read_optdef_and_apply(&options, read_optdefs, name, opt);
    else
      throw PlTypeError("option", opt);
  }
  return options;
}

PREDICATE(rocks_get, 4)
{ dbref *ref = get_rocks(A1);
  std::string value;
  auto key = get_slice(A2, ref->type.key);

  ok_or_throw_fail(ref->db->Get(read_options(A4), key->slice(), &value));
  return unify_value(A3, value, ref->builtin_merger, ref->type.value);
}

PREDICATE(rocks_delete, 3)
{ dbref *ref = get_rocks(A1);
  auto key = get_slice(A2, ref->type.key);

  ok_or_throw_fail(ref->db->Delete(write_options(A3), key->slice()));
  return true;
}

typedef enum
{ ENUM_NOT_INITIALIZED,
  ENUM_ALL,  // TODO: not used?
  ENUM_FROM, // TODO: not used?
  ENUM_PREFIX
} enum_type;

struct enum_state
{
  enum_state(dbref *_ref)
    : ref(_ref), type(ENUM_NOT_INITIALIZED)
  { }
  std::unique_ptr<rocksdb::Iterator> it;
  dbref    *ref;
  enum_type type;
  std::string prefix;
};

[[nodiscard]]
static bool
unify_enum_key(PlTerm t, const enum_state *state)
{ if ( state->type == ENUM_PREFIX )
  { rocksdb::Slice k(state->it->key());

    // TODO: use std::string's compare
    if ( k.size_ >= state->prefix.length() &&
	 memcmp(k.data_, state->prefix.data(), state->prefix.length()) == 0 )
    { k.data_ += state->prefix.length();
      k.size_ -= state->prefix.length();

      return unify(t, k, state->ref->type.key);
    } else
      return false;
  } else
  { return unify(t, state->it->key(), state->ref->type.key);
  }
}


[[nodiscard]]
static bool
enum_key_prefix(const enum_state *state)
{ if ( state->type == ENUM_PREFIX )
  { rocksdb::Slice k(state->it->key());
    return ( k.size_ >= state->prefix.length() &&
	     memcmp(k.data_, state->prefix.data(), state->prefix.length()) == 0 );
  } else
    return true;
}


[[nodiscard]]
static foreign_t
rocks_enum(PlTermv PL_av, int ac, enum_type type, PlControl handle, rocksdb::ReadOptions options)
{ PlForeignContextPtr<enum_state> state(handle);

  switch ( handle.foreign_control() )
  { case PL_FIRST_CALL:
      state.set(new enum_state(get_rocks(A1)));
      if ( ac >= 4 )
      { if ( !(state->ref->type.key == BLOB_ATOM ||
	       state->ref->type.key == BLOB_STRING ||
	       state->ref->type.key == BLOB_BINARY) )
	  throw PlPermissionError("enum", "rocksdb", A1);

	std::string prefix = A4.get_nchars(REP_UTF8|CVT_IN|CVT_EXCEPTION);

	if ( type == ENUM_PREFIX )
        { state->prefix = prefix;
	}
	state->it.reset(state->ref->db->NewIterator(options));
	state->it->Seek(prefix);
      } else
      { state->it.reset(state->ref->db->NewIterator(options));
	state->it->SeekToFirst();
      }
      state->type = type;
      [[fallthrough]];
    case PL_REDO:
    { PlFrame fr;
      for ( ; state->it->Valid(); state->it->Next() )
      { if ( unify_enum_key(A2, state.get()) &&
	     unify_value(A3, state->it->value(),
			 state->ref->builtin_merger, state->ref->type.value) )
	{ state->it->Next();
	  if ( state->it->Valid() && enum_key_prefix(state.get()) )
          { state.keep();
            PL_retry_address(state.get());
	  } else
	  { return true;
	  }
	}
	fr.rewind();
      }
      return false;
    }
    case PL_PRUNED:
      return true;
    default:
      assert(0);
      return false;
  }
  return false;
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

static void
batch_operation(const dbref *ref, rocksdb::WriteBatch &batch, PlTerm e)
{ PlAtom name(PlAtom::null);
  size_t arity;

  static PlAtom ATOM_delete("delete");
  static PlAtom ATOM_put("put");

  PlCheckFail(e.name_arity(&name, &arity));
  if ( ATOM_delete == name && arity == 1 )
  { auto key = get_slice(e[1], ref->type.key);
    batch.Delete(key->slice());
  } else if ( ATOM_put == name && arity == 2 )
  { auto key = get_slice(e[1], ref->type.key);
    auto value = get_slice(e[2], ref->type.value);
    batch.Put(key->slice(), value->slice());
  } else
  { throw PlDomainError("rocks_batch_operation", e);
  }
}


PREDICATE(rocks_batch, 3)
{ dbref *ref = get_rocks(A1);
  rocksdb::WriteBatch batch;
  PlTerm_tail tail(A2);
  PlTerm_var e;

  while ( tail.next(e) )
  { batch_operation(ref, batch, e);
  }

  ok_or_throw_fail(ref->db->Write(write_options(A3), &batch));
  return true;
}


static PlAtom ATOM_estimate_num_keys("estimate_num_keys");

PREDICATE(rocks_property, 3)
{ dbref *ref = get_rocks(A1);

  PlAtom prop = A2.as_atom();
  if ( ATOM_estimate_num_keys == prop )
  { uint64_t value;

    return ref->db->GetIntProperty("rocksdb.estimate-num-keys", &value) &&
	     A3.unify_integer(value);
  } else
     throw PlDomainError("rocks_property", A2);
}
